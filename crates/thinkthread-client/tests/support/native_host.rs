//! Opt-in native assembly using only the public Management SDK.
use super::*;
use std::io::{BufRead, Write};

async fn attach(config: ManagementConfig, root: &str, child: &sdk::ThinkThreadView) -> Fixture {
    let mut f = Fixture::seed(false).await;
    f.config = config;
    f.policy.paths.fs_relative_workspace = ".".into();
    f.policy.backend_release_ref = "alpha11/0.1.0-25".into();
    f.policy.evidence_refs = vec!["native-public-sdk".into()];
    f.policy.frozen_host_config_digest = management_config_digest(&f.config);
    f.policy.max_file_bytes = 131072;
    f.policy.max_inflight = 512;
    f.policy.max_reserved_bytes = 134217728;
    f.target.root_id = root.into();
    f.target.native_child_id = child.thinkthread_id.clone();
    f.target.branch_id = child.fs_branch_id.clone().unwrap();
    f.target.workspace_mapping_digest = host_digest(&f.policy.paths);
    let c = f.conn();
    c.execute("UPDATE joint_bindings SET root_id=?1", [root])
        .unwrap();
    c.execute("UPDATE joint_roots SET id=?1", [root]).unwrap();
    c.execute("UPDATE joint_objects SET root_id=?1,native_id=CASE kind WHEN 'root' THEN ?1 WHEN 'child' THEN ?2 ELSE ?3 END", rusqlite::params![root,child.thinkthread_id,f.target.branch_id]).unwrap();
    c.execute("UPDATE joint_fences SET root_id=?1,id=?1", [root])
        .unwrap();
    f.repo
        .register_host_target(f.target.clone(), f.policy.clone())
        .await
        .unwrap();
    f
}

async fn sdk_call(c: &sdk::ThinkThreadClient, method: &str, p: Value, op: String) -> Value {
    macro_rules! read {
        ($g:ident, $m:ident) => { match c.$g().$m(serde_json::from_value(p).unwrap()).await {
            Ok(v) => json!({"ok":true,"result":v}), Err(e) => json!({"ok":false,"error":e.to_string()})
        }};
    }
    macro_rules! mutate {
        ($g:ident, $m:ident) => { match c.$g().$m(op, serde_json::from_value(p).unwrap()).await {
            Ok(v) => json!({"ok":true,"result":v}), Err(e) => json!({"ok":false,"error":e.to_string()})
        }};
    }
    match method {
        "thinkthread.create" => mutate!(thinkthreads, create_with_operation),
        "thinkthread.start" => mutate!(thinkthreads, start_with_operation),
        "thinkthread.fork" => mutate!(thinkthreads, fork_with_operation),
        "thinkthread.stop" => mutate!(thinkthreads, stop_with_operation),
        "thinkthread.force_stop" => mutate!(thinkthreads, force_stop_with_operation),
        "thinkthread.remove" => mutate!(thinkthreads, remove_with_operation),
        "thinkthread.get" => read!(thinkthreads, get),
        "thinkthread.exec" => mutate!(thinkthreads, exec_with_operation),
        "thinkthread.read" => read!(thinkthreads, read),
        "message.send" => mutate!(messages, send_with_operation),
        "message.get" => read!(messages, get),
        "message.wait" => read!(messages, wait),
        "operation.get" => match c.get_operation(serde_json::from_value(p).unwrap()).await {
            Ok(value) => json!({"ok":true,"result":value}),
            Err(error) => json!({"ok":false,"error":error.to_string()}),
        },
        _ => json!({"ok":false,"error":"unsupported test method"}),
    }
}

pub async fn bridge() {
    let path =
        std::env::var_os("NATIVE_HOST_CONFIG").expect("explicit native configuration required");
    let v: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let root = PathBuf::from(v["context"].as_str().unwrap());
    let config = ManagementConfig {
        host_executable: PathBuf::from(v["host_executable"].as_str().unwrap()),
        state_root: root.join("state"),
        runtime_root: root.join("r"),
        config_root: root.join("config"),
        cache_root: root.join("cache"),
        working_directory: root.join("workspace"),
        fs_root: root.join("workspace"),
        home_directory: PathBuf::from(v["home"].as_str().unwrap()),
        executable_search_path: vec!["/usr/bin".into(), "/bin".into()],
        base_environment: v["environment_keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|k| {
                let k = k.as_str().unwrap();
                (k.into(), std::env::var(k).unwrap())
            })
            .collect(),
        connect_timeout: Duration::from_secs(30),
    };
    if let Some(path) = std::env::var_os("NATIVE_HOST_CLAIM_FILE") {
        let q: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let repository = repository(Path::new(q["db"].as_str().unwrap()));
        let token: HostAccessTokenV1 = serde_json::from_value(q["token"].clone()).unwrap();
        assert!(repository.claim_host_step(token, 0).await.unwrap());
        let n: JointNativeIntent = serde_json::from_value(q["native"].clone()).unwrap();
        let client = config.connect().await.unwrap();
        let result = if n.method == "thinkthread.write" {
            let request = sdk::ThinkThreadWriteRequest {
                thinkthread_id: n.params["thinkthreadId"].as_str().unwrap().into(),
                path: n.params["path"].as_str().unwrap().into(),
                max_bytes: n.params["maxBytes"].as_u64().unwrap() as u32,
                data_base64: q["data_base64"].as_str().unwrap().into(),
            };
            serde_json::to_value(
                client
                    .thinkthreads()
                    .write_with_operation(n.operation_id, request)
                    .await
                    .unwrap(),
            )
            .unwrap()
        } else {
            serde_json::to_value(
                client
                    .thinkthreads()
                    .exec_with_operation(n.operation_id, serde_json::from_value(n.params).unwrap())
                    .await
                    .unwrap(),
            )
            .unwrap()
        };
        println!("NATIVE_MUTATION {result}");
        std::io::stdout().flush().unwrap();
        std::process::exit(23);
    }
    let client = config.connect().await.unwrap();
    let mut bindings: HashMap<String, Fixture> = HashMap::new();
    let mut applications: HashMap<String, SessionApplication> = HashMap::new();
    let mut held: HashMap<String, JointHostAccess> = HashMap::new();
    println!(
        "NATIVE {}",
        json!({"ready":true,"fingerprint":sdk::MANAGEMENT_CONTRACT_FINGERPRINT})
    );
    std::io::stdout().flush().unwrap();
    for line in std::io::stdin().lock().lines() {
        let q: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let method = q["method"].as_str().unwrap();
        let response = if method == "attach" {
            let child: sdk::ThinkThreadView = serde_json::from_value(q["child"].clone()).unwrap();
            let f = attach(config.clone(), q["root"].as_str().unwrap(), &child).await;
            let r = json!({"ok":true,"target":f.target,"policy":f.policy,"db":f.root.path().join("db")});
            applications.insert(q["binding"].as_str().unwrap().into(), application(&f));
            bindings.insert(q["binding"].as_str().unwrap().into(), f);
            r
        } else if method == "public" {
            let app = &applications[q["binding"].as_str().unwrap()];
            let ctx = SecurityContext::tenant("a", "principal");
            let request = q["request"].clone();
            let result = match q["operation"].as_str().unwrap() {
                "read" => app
                    .read_file(&ctx, serde_json::from_value(request).unwrap())
                    .await
                    .map(|v| serde_json::to_value(v).unwrap()),
                "write" => app
                    .write_file(&ctx, serde_json::from_value(request).unwrap())
                    .await
                    .map(|v| serde_json::to_value(v).unwrap()),
                "exec" => app
                    .exec(&ctx, serde_json::from_value(request).unwrap())
                    .await
                    .map(|v| serde_json::to_value(v).unwrap()),
                _ => panic!("unknown public test request"),
            };
            json!({"ok":result.is_ok(),"result":result.as_ref().ok(),"error":result.err().map(|e|e.to_string())})
        } else if method == "host"
            || method == "hold"
            || method == "recover"
            || method == "snapshot"
            || method == "held"
            || method == "crash"
            || method == "race"
        {
            let f = &bindings[q["binding"].as_str().unwrap()];
            let id = q["intent_id"].as_str().unwrap_or("snapshot");
            let before = f.snapshot();
            let owner = CandidateHostOwner::new(
                config.clone(),
                "candidate-fixture".into(),
                "fixed".into(),
                Duration::from_millis(q["observation_timeout_ms"].as_u64().unwrap_or(330000)),
            )
            .unwrap();
            let repository = repository(&f.root.path().join("db"));
            let result: Result<Value, SessionDomainError> = if method == "snapshot" {
                Ok(json!({"snapshot":snapshot_digest(&before)}))
            } else if method == "recover" {
                owner.recover_host(id.into(), repository).await
            } else {
                let r = &q["request"];
                let request = match r["kind"].as_str().unwrap() {
                    "read" => JointHostRequest::Read {
                        path: r["path"].as_str().unwrap().into(),
                    },
                    "write" => JointHostRequest::Write {
                        path: r["path"].as_str().unwrap().into(),
                        content: STANDARD.decode(r["data_base64"].as_str().unwrap()).unwrap(),
                        mode: match r["mode"].as_str().unwrap() {
                            "overwrite" => WriteMode::Overwrite,
                            "create" => WriteMode::Create,
                            "atomic_overwrite" => WriteMode::AtomicOverwrite,
                            _ => panic!("unknown mode"),
                        },
                        create_parents: r["create_parents"].as_bool().unwrap(),
                    },
                    "exec" => JointHostRequest::Exec {
                        request: ExecRequest {
                            command: r["request"]["command"].as_str().unwrap().into(),
                            args: r["request"]["args"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|v| v.as_str().unwrap().into())
                                .collect(),
                            cwd: r["request"]["cwd"].as_str().map(|s| BackendPath(s.into())),
                            timeout_ms: r["request"]["timeout_ms"].as_u64(),
                            env: r["request"]["env"].as_object().map(|v| {
                                v.iter()
                                    .map(|(k, v)| (k.clone(), v.as_str().unwrap().into()))
                                    .collect()
                            }),
                            extra: r["request"].get("extra").cloned(),
                            ..Default::default()
                        },
                        required_output_bytes: r["required_output_bytes"]
                            .as_u64()
                            .map(|v| v as u32),
                    },
                    "export" => JointHostRequest::Export {
                        path: r["path"].as_str().unwrap().into(),
                        preferred_name: None,
                    },
                    _ => panic!("unknown native request"),
                };
                if method == "race" {
                    let one = format!("{id}-one");
                    let two = format!("{id}-two");
                    let repo1 = repository.clone();
                    let repo2 = super::repository(&f.root.path().join("db"));
                    let (a, b) = tokio::join!(
                        owner.prepare_host_operation(
                            f.context(&one),
                            f.record.clone(),
                            request.clone(),
                            repo1
                        ),
                        owner.prepare_host_operation(
                            f.context(&two),
                            f.record.clone(),
                            request.clone(),
                            repo2
                        )
                    );
                    assert_ne!(a.is_ok(), b.is_ok());
                    let (winner, access, error) = if let Ok(a) = a {
                        (one, a, b.err().unwrap())
                    } else {
                        (two, b.unwrap(), a.err().unwrap())
                    };
                    let value = json!({"ok":true,"winner":winner,"access":access.access,"loser_error":error.to_string()});
                    held.insert(winner, access);
                    println!("NATIVE {value}");
                    std::io::stdout().flush().unwrap();
                    continue;
                }
                let access = if method == "held" {
                    Ok(held.remove(id).unwrap())
                } else {
                    owner
                        .prepare_host_operation(
                            f.context(id),
                            f.record.clone(),
                            request.clone(),
                            repository,
                        )
                        .await
                };
                match access {
                    Err(e) => Err(e),
                    Ok(a) if method == "crash" => {
                        let h = f.intent(id).await;
                        let path = f.root.path().join("claim.json");
                        std::fs::write(&path,serde_json::to_vec(&json!({"db":f.root.path().join("db"),"token":a.access,"native":h.prepared.native[0],"data_base64":q["request"]["data_base64"]})).unwrap()).unwrap();
                        let output=std::process::Command::new(std::env::current_exe().unwrap()).args(["native_current_view_host_bridge","--ignored","--nocapture"]).env("NATIVE_HOST_CLAIM_FILE",path).output().unwrap();
                        assert_eq!(output.status.code(),Some(23));
                        Ok(json!({"child_exit_code":23,"raw":String::from_utf8_lossy(&output.stdout)}))
                    }
                    Ok(a) if method == "hold" => { held.insert(id.into(),a);Ok(json!({"prepared":true})) },
                    Ok(a) => match request {
                        JointHostRequest::Write { .. } => a.backend.write_host(request).await.map(|n|json!({"bytes_written":n})),
                        JointHostRequest::Read { path } => a.backend.files().read_bytes(ReadBytesRequest {path:BackendPath(path)}).await.map(|b|json!({"data_base64":STANDARD.encode(b)})).map_err(|e|host_unavailable(&e.to_string())),
                        JointHostRequest::Exec {request,..} => a.backend.exec().exec(request).await.map(|r|json!({"stdout_base64":STANDARD.encode(r.stdout),"stderr_base64":STANDARD.encode(r.stderr),"exit_code":r.exit_code,"timed_out":r.timed_out})).map_err(|e|host_unavailable(&e.to_string())),
                        _ => Err(unsupported_joint("native_export")),
                    }
                }
            };
            let intent = f.repo.host_intent(id.into()).await.ok();
            json!({"ok":result.is_ok(),"result":result.as_ref().ok(),"error":result.err().map(|e|e.to_string()),"intent":intent,"before":snapshot_digest(&before),"after":snapshot_digest(&f.snapshot())})
        } else {
            sdk_call(
                &client,
                method,
                q["params"].clone(),
                q["operation_id"].as_str().unwrap_or("").into(),
            )
            .await
        };
        if let Some(directory) = v["evidence_dir"].as_str() {
            let directory = Path::new(directory).join("sqlite");
            std::fs::create_dir_all(&directory).unwrap();
            for (name, f) in &bindings {
                let path = directory.join(format!("{name}.db"));
                if path.exists() {
                    std::fs::remove_file(&path).unwrap();
                }
                f.conn()
                    .execute("VACUUM INTO ?1", [path.to_str().unwrap()])
                    .unwrap();
            }
        }
        println!("NATIVE {response}");
        std::io::stdout().flush().unwrap();
    }
    client.close().await.unwrap();
}

fn snapshot_digest(snapshot: &[(String, Vec<Vec<String>>)]) -> Value {
    json!(snapshot
        .iter()
        .map(|(name, rows)| (
            name.clone(),
            json!({"rows":rows.len(),"sha256":host_digest(rows)})
        ))
        .collect::<BTreeMap<_, _>>())
}
