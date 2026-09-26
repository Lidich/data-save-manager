use serde_json::Value;

const MAX_LOCATIONS: usize = 16;
const MAX_PATH_CHARS: usize = 256;

#[derive(Debug, Default)]
pub struct JsonNulRepair {
    pub nul_characters: usize,
    pub affected_values: usize,
    pub escaped_keys: usize,
    pub locations: Vec<String>,
}

pub(super) fn remove_nuls(rows: &mut Value) -> JsonNulRepair {
    let mut report = JsonNulRepair::default();
    visit(rows, &mut String::new(), &mut report);
    report
}

fn record_location(path: &str, report: &mut JsonNulRepair) {
    if report.locations.len() < MAX_LOCATIONS {
        report
            .locations
            .push(path.chars().take(MAX_PATH_CHARS).collect());
    }
}

fn push_key(path: &mut String, key: &str) {
    path.push('/');
    for c in key.chars() {
        match c {
            '~' => path.push_str("~0"),
            '/' => path.push_str("~1"),
            c => path.push(c),
        }
    }
}

fn visit(value: &mut Value, path: &mut String, report: &mut JsonNulRepair) {
    match value {
        Value::String(text) => {
            let count = text.bytes().filter(|byte| *byte == 0).count();
            if count > 0 {
                text.retain(|character| character != '\0');
                report.nul_characters += count;
                report.affected_values += 1;
                record_location(path, report);
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter_mut().enumerate() {
                let length = path.len();
                path.push('/');
                path.push_str(&index.to_string());
                visit(value, path, report);
                path.truncate(length);
            }
        }
        Value::Object(object) => {
            if object.keys().any(|key| key.contains('\0')) {
                *object = std::mem::take(object)
                    .into_iter()
                    .map(|(key, value)| {
                        report.nul_characters += key.bytes().filter(|byte| *byte == 0).count();
                        let escaped = key.replace('\\', "\\\\").replace('\0', "\\u0000");
                        if escaped != key {
                            report.escaped_keys += 1;
                            let length = path.len();
                            push_key(path, &escaped);
                            record_location(path, report);
                            path.truncate(length);
                        }
                        (escaped, value)
                    })
                    .collect();
            }
            for (key, value) in object {
                let length = path.len();
                push_key(path, key);
                visit(value, path, report);
                path.truncate(length);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn removes_only_nul_values_and_reports_bounded_json_pointers() {
        let mut value = json!([{"name":"A\0Б🚀", "a/b~c":["\0\0", "literal \\u0000"], "n":null, "amount":123, "controls":"\n\r\t\u{001b}"}]);
        let report = remove_nuls(&mut value);
        assert_eq!(report.nul_characters, 3);
        assert_eq!(report.affected_values, 2);
        assert_eq!(report.escaped_keys, 0);
        assert!(report.locations.contains(&"/0/name".into()));
        assert!(report.locations.contains(&"/0/a~1b~0c/0".into()));
        assert_eq!(value[0]["name"], "AБ🚀");
        assert_eq!(value[0]["a/b~c"][0], "");
        assert_eq!(value[0]["a/b~c"][1], "literal \\u0000");
        assert_eq!(value[0]["controls"], "\n\r\t\u{001b}");
        assert_eq!(value[0]["amount"], 123);
        assert!(value[0]["n"].is_null());
        assert_eq!(remove_nuls(&mut value).nul_characters, 0);
        let mut many = json!(vec!["\0"; 50]);
        let report = remove_nuls(&mut many);
        assert_eq!(report.locations.len(), MAX_LOCATIONS);
        assert_eq!(report.affected_values, 50);
        let mut long = json!({"я".repeat(300):"\0"});
        assert_eq!(
            remove_nuls(&mut long).locations[0].chars().count(),
            MAX_PATH_CHARS
        );
    }

    #[test]
    fn escapes_nested_nul_keys_without_collisions_and_is_idempotent() {
        let mut value = json!({"metadata": {
            "x\0": 1, "x": 2, "x\\u0000": 3, "x\\\\u0000": 4,
            "nested": [{"\0": "value\0"}]
        }});
        let report = remove_nuls(&mut value);
        assert_eq!(report.nul_characters, 3);
        assert_eq!(report.affected_values, 1);
        assert_eq!(report.escaped_keys, 4);
        let metadata = &value["metadata"];
        assert_eq!(metadata.as_object().unwrap().len(), 5);
        assert_eq!(metadata["x\\u0000"], 1);
        assert_eq!(metadata["x"], 2);
        assert_eq!(metadata["x\\\\u0000"], 3);
        assert_eq!(metadata["x\\\\\\\\u0000"], 4);
        assert_eq!(metadata["nested"][0]["\\u0000"], "value");
        let repaired = value.clone();
        let second = remove_nuls(&mut value);
        assert_eq!(second.nul_characters, 0);
        assert_eq!(second.escaped_keys, 0);
        assert_eq!(value, repaired);
    }

    #[test]
    fn clean_objects_keep_literal_backslashes_in_keys() {
        let mut value = json!({"metadata": {"x\\u0000": "literal", "x\\y": "other"}, "bad":"\0"});
        let original_metadata = value["metadata"].clone();
        let report = remove_nuls(&mut value);
        assert_eq!(report.escaped_keys, 0);
        assert_eq!(value["metadata"], original_metadata);
    }
}
