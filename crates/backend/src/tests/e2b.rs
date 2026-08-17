
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::{symlink, PermissionsExt};

    fn extract_bytes(bytes: &[u8], destination: &Path) {
        let mut archive = tar::Archive::new(bytes);
        archive.unpack(destination).expect("extract archive");
    }

    #[test]
    fn empty_inputs_produce_empty_replacement_roots() {
        let (bytes, _sha256, _size, skills) =
            build_archive(None, &[], "/home/user/workspace", "/home/user/.xgovernor/skills")
                .expect("empty bootstrap");
        let extracted = tempfile::tempdir().expect("extract dir");
        extract_bytes(&bytes, extracted.path());

        assert!(extracted.path().join("workspace").is_dir());
        assert!(extracted.path().join("skills").is_dir());
        assert_eq!(skills, Vec::new());
    }

    #[cfg(unix)]
    #[test]
    fn snapshots_hidden_git_empty_modes_and_symlinks() {
        let host = tempfile::tempdir().expect("host");
        let workspace = host.path().join("workspace");
        fs::create_dir_all(workspace.join(".git")).expect("git dir");
        fs::create_dir(workspace.join("empty")).expect("empty dir");
        fs::write(workspace.join(".hidden"), "hidden").expect("hidden file");
        fs::write(workspace.join(".git/config"), "git config").expect("git config");
        fs::write(workspace.join("run.sh"), "#!/bin/sh\n").expect("script");
        fs::set_permissions(workspace.join("run.sh"), fs::Permissions::from_mode(0o755))
            .expect("chmod");
        symlink("run.sh", workspace.join("run-link")).expect("internal symlink");

        let skill_a = host.path().join("skill-a");
        fs::create_dir_all(skill_a.join("assets")).expect("skill dir");
        fs::write(skill_a.join("SKILL.md"), "---\nname: a\n---\nprompt").expect("manifest");
        fs::write(skill_a.join("assets/data.txt"), "asset").expect("asset");

        let skill_b = host.path().join("skill-b");
        fs::create_dir_all(&skill_b).expect("skill dir");
        fs::write(skill_b.join("SKILL.md"), "---\nname: b\n---\nprompt").expect("manifest");

        let workspace = canonicalize_bootstrap_dir(&workspace).expect("workspace canonical");
        let skill_a = canonicalize_bootstrap_dir(&skill_a).expect("skill a canonical");
        let skill_b = canonicalize_bootstrap_dir(&skill_b).expect("skill b canonical");

        let (bytes, _sha256, _size, skills) = build_archive(
            Some(&workspace),
            &[skill_a.clone(), skill_b.clone()],
            "/home/user/workspace",
            "/home/user/.xgovernor/skills",
        )
        .expect("bootstrap");
        let extracted = tempfile::tempdir().expect("extract dir");
        extract_bytes(&bytes, extracted.path());

        assert_eq!(
            fs::read_to_string(extracted.path().join("workspace/.git/config")).unwrap(),
            "git config"
        );
        assert!(extracted.path().join("workspace/empty").is_dir());
        assert_eq!(
            fs::metadata(extracted.path().join("workspace/run.sh"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
        assert_eq!(
            fs::read_link(extracted.path().join("workspace/run-link")).unwrap(),
            PathBuf::from("run.sh")
        );

        assert_eq!(skills.len(), 2);
        assert_eq!(skills[0].source, skill_a);
        assert_eq!(skills[0].remote_dir, "/home/user/.xgovernor/skills/skill-00000");
        assert_eq!(skills[1].source, skill_b);
        assert_eq!(skills[1].remote_dir, "/home/user/.xgovernor/skills/skill-00001");
        assert_eq!(
            fs::read_to_string(extracted.path().join("skills/skill-00000/assets/data.txt"))
                .unwrap(),
            "asset"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_that_escapes_workspace() {
        let host = tempfile::tempdir().expect("host");
        let workspace = host.path().join("workspace");
        fs::create_dir(&workspace).expect("workspace");
        fs::write(host.path().join("secret"), "secret").expect("secret");
        symlink("../secret", workspace.join("escape")).expect("symlink");
        let workspace = canonicalize_bootstrap_dir(&workspace).expect("canonical");

        let error = build_archive(
            Some(&workspace),
            &[],
            "/home/user/workspace",
            "/home/user/.xgovernor/skills",
        )
        .expect_err("escaping link must fail");
        assert!(matches!(error, E2bBootstrapError::InvalidPath { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_fifo_special_file() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let host = tempfile::tempdir().expect("host");
        let workspace = host.path().join("workspace");
        fs::create_dir(&workspace).expect("workspace");
        let fifo = workspace.join("pipe");
        let path = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        let workspace = canonicalize_bootstrap_dir(&workspace).expect("canonical");

        let error = build_archive(
            Some(&workspace),
            &[],
            "/home/user/workspace",
            "/home/user/.xgovernor/skills",
        )
        .expect_err("FIFO must fail");
        assert!(matches!(error, E2bBootstrapError::InvalidPath { .. }));
    }

    #[test]
    fn enforces_all_capacity_counters() {
        let mut limits = ArchiveLimits {
            entries: E2B_BOOTSTRAP_MAX_ENTRIES,
            total_bytes: 0,
        };
        assert!(matches!(
            limits.add_entry(Path::new("overflow")),
            Err(E2bBootstrapError::CapacityExceeded { .. })
        ));

        let mut limits = ArchiveLimits::default();
        assert!(matches!(
            limits.add_file(Path::new("large"), E2B_BOOTSTRAP_MAX_FILE_BYTES + 1),
            Err(E2bBootstrapError::CapacityExceeded { .. })
        ));

        let mut limits = ArchiveLimits {
            entries: 0,
            total_bytes: E2B_BOOTSTRAP_MAX_TOTAL_BYTES,
        };
        assert!(matches!(
            limits.add_file(Path::new("total"), 1),
            Err(E2bBootstrapError::CapacityExceeded { .. })
        ));
    }

    #[test]
    fn distinct_workspaces_produce_distinct_digests() {
        let host = tempfile::tempdir().expect("host");
        let cz = host.path().join("cz");
        let xxy = host.path().join("xxy");
        fs::create_dir(&cz).unwrap();
        fs::create_dir(&xxy).unwrap();
        fs::write(cz.join("owner.txt"), "cz").unwrap();
        fs::write(xxy.join("owner.txt"), "xxy").unwrap();
        let cz = canonicalize_bootstrap_dir(&cz).unwrap();
        let xxy = canonicalize_bootstrap_dir(&xxy).unwrap();

        let (_bytes, cz_sha256, ..) = build_archive(
            Some(&cz),
            &[],
            "/home/user/workspace",
            "/home/user/.xgovernor/skills",
        )
        .unwrap();
        let (_bytes, xxy_sha256, ..) = build_archive(
            Some(&xxy),
            &[],
            "/home/user/workspace",
            "/home/user/.xgovernor/skills",
        )
        .unwrap();

        assert_ne!(cz_sha256, xxy_sha256);
    }
}
