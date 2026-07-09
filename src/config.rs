use std::collections::HashMap;
use std::fs;

pub const LOCAL_ENV_CONFIG: &str = "/opt/2prestashopsync/.env";
pub const PROTECTED_MARIADB_HOSTS: [&str; 1] = ["www.mercaboy.com"];

pub fn config_value(config: &HashMap<String, String>, key: &str, default: &str) -> String {
    config
        .get(key)
        .or_else(|| config.get(&key.to_lowercase()))
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| default.to_string())
}

pub fn read_key_value_file(path: &str, values: &mut HashMap<String, String>) {
    if let Ok(contents) = fs::read_to_string(path) {
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim().to_string();
                let value = value
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_string();
                values.insert(key.clone(), value.clone());
                values.insert(key.to_lowercase(), value);
            }
        }
    }
}

pub fn read_local_config() -> HashMap<String, String> {
    let mut values = HashMap::new();
    read_key_value_file(LOCAL_ENV_CONFIG, &mut values);

    values
}

pub fn is_protected_mariadb_host(host: &str) -> bool {
    let host = host.trim().to_lowercase();
    PROTECTED_MARIADB_HOSTS
        .iter()
        .any(|protected| host == *protected)
}
