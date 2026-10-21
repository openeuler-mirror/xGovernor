use serde_json::{json, Value};
use std::{collections::BTreeMap, os::unix::fs::MetadataExt, path::Path};

pub fn database(path: &Path) -> BTreeMap<String, Value> {
    let db = rusqlite::Connection::open(path).unwrap();
    let tables: Vec<String> = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ('local_deployment','execution_configurations','execution_defaults') ORDER BY name").unwrap().query_map([], |r|r.get(0)).unwrap().collect::<Result<_,_>>().unwrap();
    tables
        .into_iter()
        .map(|name| {
            let mut statement = db
                .prepare(&format!("SELECT * FROM \"{name}\" ORDER BY rowid"))
                .unwrap();
            let columns = statement.column_count();
            let rows: Vec<Vec<String>> = statement
                .query_map([], |r| {
                    Ok((0..columns)
                        .map(|i| format!("{:?}", r.get_ref(i).unwrap()))
                        .collect())
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            (name, json!(rows))
        })
        .collect()
}
/// Transport trace/state files and SQLite physical files are observed separately.
pub fn tree(root: &Path, excluded: &[&str]) -> Value {
    fn walk(root: &Path, path: &Path, excluded: &[&str], result: &mut BTreeMap<String, Value>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            let name = path.strip_prefix(root).unwrap().to_str().unwrap();
            if excluded.contains(&name) {
                continue;
            }
            let metadata = path.symlink_metadata().unwrap();
            let value = if metadata.is_dir() {
                json!({"kind":"directory","mode":metadata.mode()})
            } else if metadata.file_type().is_symlink() {
                json!({"kind":"symlink","target":std::fs::read_link(&path).unwrap()})
            } else if !metadata.is_file() {
                json!({"kind":"special","mode":metadata.mode()})
            } else {
                json!({"kind":"file","mode":metadata.mode(),"bytes":std::fs::read(&path).unwrap()})
            };
            result.insert(name.into(), value);
            if metadata.is_dir() {
                walk(root, &path, excluded, result);
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(root, root, excluded, &mut result);
    json!(result)
}
