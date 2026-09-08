// Copyright (C) 2026 Tencent. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Parse `GoosefsConfig` the same way the Python binding does.

use std::collections::HashMap;

use goosefs_sdk::config::GoosefsConfig;
use jni::jni_sig;
use jni::jni_str;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::sys::jboolean;
use jni::Env;
use jni::EnvUnowned;

use crate::convert::{
    boxed_integer, jmap_to_hashmap, millis_as_jlong, optional_jstring_to_string, string_list,
};
use crate::error::ThrowException;
use crate::Result;

/// Build a config from Java constructor seeds, then overlay `GOOSEFS_*`.
pub(crate) fn parse_config(
    master_addr: Option<&str>,
    properties: Option<&HashMap<String, String>>,
    file_content: Option<&str>,
) -> Result<GoosefsConfig> {
    let mut cfg = if let Some(content) = file_content.filter(|s| !s.is_empty()) {
        GoosefsConfig::from_properties_str(content)
    } else {
        let master_addr = master_addr.unwrap_or("").trim();
        if master_addr.is_empty() {
            return Err(crate::Error::Config(
                "master_addr must be a non-empty 'host:port', comma-separated list, or 'gfs://' URI"
                    .into(),
            ));
        }
        if master_addr.starts_with("gfs://") {
            GoosefsConfig::from_uri(master_addr).map_err(|e| crate::Error::Config(e.to_string()))?
        } else {
            let addrs: Vec<String> = master_addr
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if addrs.is_empty() {
                return Err(crate::Error::Config(
                    "master_addr must be a non-empty 'host:port', comma-separated list, or 'gfs://' URI"
                        .into(),
                ));
            }
            if addrs.len() == 1 {
                GoosefsConfig::new(&addrs[0])
            } else {
                GoosefsConfig::new_ha(addrs)
            }
        }
    };

    if let Some(props) = properties {
        if !props.is_empty() {
            let mut buf = String::new();
            for (key, val) in props {
                if val.contains('\n') {
                    return Err(crate::Error::Config(format!(
                        "property value for {key:?} may not contain newline"
                    )));
                }
                buf.push_str(key);
                buf.push('=');
                buf.push_str(val);
                buf.push('\n');
            }
            let parsed = GoosefsConfig::from_properties_str(&buf);
            let preserved_addr = cfg.master_addr.clone();
            let preserved_addrs = std::mem::take(&mut cfg.master_addrs);
            let preserved_root = std::mem::take(&mut cfg.root);
            cfg = parsed;
            if cfg.master_addr.is_empty() {
                cfg.master_addr = preserved_addr;
            }
            if cfg.master_addrs.is_empty() {
                cfg.master_addrs = preserved_addrs;
            }
            if cfg.root.is_empty() {
                cfg.root = preserved_root;
            }
        }
    }

    Ok(cfg.apply_env())
}

pub(crate) fn parse_config_from_java(env: &mut Env, config: &JObject) -> Result<GoosefsConfig> {
    let seed_addr = env
        .call_method(
            config,
            jni_str!("seedMasterAddr"),
            jni_sig!("()Ljava/lang/String;"),
            &[],
        )?
        .l()?;
    let seed_props = env
        .call_method(
            config,
            jni_str!("seedProperties"),
            jni_sig!("()Ljava/util/Map;"),
            &[],
        )?
        .l()?;
    let seed_file = env
        .call_method(
            config,
            jni_str!("seedFileContent"),
            jni_sig!("()Ljava/lang/String;"),
            &[],
        )?
        .l()?;

    let master_addr = optional_jstring_to_string(env, &seed_addr)?;
    let properties = jmap_to_hashmap(env, &seed_props)?;
    let file_content = optional_jstring_to_string(env, &seed_file)?;
    parse_config(
        master_addr.as_deref(),
        Some(&properties),
        file_content.as_deref(),
    )
}

fn make_resolved<'local>(env: &mut Env<'local>, cfg: &GoosefsConfig) -> Result<JObject<'local>> {
    let master_addr = env.new_string(&cfg.master_addr)?;
    let master_addrs = string_list(env, &cfg.master_addresses())?;
    let root = env.new_string(&cfg.root)?;
    let auth_type = env.new_string(cfg.auth_type.to_string())?;
    let auth_username = env.new_string(&cfg.auth_username)?;
    let write_type = boxed_integer(env, cfg.write_type)?;
    Ok(env.new_object(
        jni_str!("com/tencent/goosefs/Config$Resolved"),
        jni_sig!("(Ljava/lang/String;Ljava/util/List;JJLjava/lang/String;ZLjava/lang/String;Ljava/lang/String;ZJJLjava/lang/Integer;III)V"),
        &[
            JValue::Object(&master_addr),
            JValue::Object(&master_addrs),
            JValue::Long(cfg.block_size as i64),
            JValue::Long(cfg.chunk_size as i64),
            JValue::Object(&root),
            JValue::Bool(cfg.use_vpc_mapping as jboolean),
            JValue::Object(&auth_type),
            JValue::Object(&auth_username),
            JValue::Bool(cfg.metrics_enabled as jboolean),
            JValue::Long(millis_as_jlong(cfg.connect_timeout.as_millis())),
            JValue::Long(millis_as_jlong(cfg.request_timeout.as_millis())),
            JValue::Object(&write_type),
            JValue::Int(cfg.file_replication_number),
            JValue::Int(cfg.file_replication_durable),
            JValue::Int(cfg.file_replication_durable_min),
        ],
    )?)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_tencent_goosefs_Config_nativeResolve<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    master_addr: JObject<'local>,
    properties: JObject<'local>,
    file_content: JObject<'local>,
) -> JObject<'local> {
    env.with_env(|env| intern_resolve(env, master_addr, properties, file_content))
        .resolve::<ThrowException>()
}

fn intern_resolve<'local>(
    env: &mut Env<'local>,
    master_addr: JObject,
    properties: JObject,
    file_content: JObject,
) -> Result<JObject<'local>> {
    let master_addr = optional_jstring_to_string(env, &master_addr)?;
    let properties = jmap_to_hashmap(env, &properties)?;
    let file_content = optional_jstring_to_string(env, &file_content)?;
    let cfg = parse_config(
        master_addr.as_deref(),
        Some(&properties),
        file_content.as_deref(),
    )?;
    make_resolved(env, &cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ha_form_accepts_comma_separated_addresses() {
        let cfg = parse_config(Some("m1:9200, m2:9200 ,m3:9200"), None, None).unwrap();
        assert_eq!(cfg.master_addr, "m1:9200");
        assert_eq!(cfg.master_addresses().len(), 3);
    }

    #[test]
    fn empty_address_is_rejected() {
        assert!(parse_config(Some("  , ,"), None, None).is_err());
        assert!(parse_config(Some(""), None, None).is_err());
    }

    #[test]
    fn uri_form_populates_addresses_and_root() {
        let cfg = parse_config(
            Some("gfs://172.16.16.27:9200,172.16.16.23:9200,172.16.16.38:9200/xxxx"),
            None,
            None,
        )
        .unwrap();
        assert_eq!(cfg.master_addr, "172.16.16.27:9200");
        assert_eq!(cfg.master_addresses().len(), 3);
        assert_eq!(cfg.root, "/xxxx");
    }

    #[test]
    fn properties_overlay_block_size() {
        let mut props = HashMap::new();
        props.insert("goosefs.user.block.size.bytes.default".into(), "8MB".into());
        let cfg = parse_config(Some("127.0.0.1:9200"), Some(&props), None).unwrap();
        assert_eq!(cfg.block_size, 8 * 1024 * 1024);
    }

    #[test]
    fn client_cache_properties_flow_through() {
        let mut props = HashMap::new();
        props.insert("goosefs.user.client.cache.enabled".into(), "true".into());
        props.insert("goosefs.user.client.cache.page.size".into(), "65536".into());
        props.insert("goosefs.user.client.cache.size".into(), "64MB".into());
        props.insert(
            "goosefs.user.client.cache.dirs".into(),
            "/tmp/gfs-cache".into(),
        );
        props.insert(
            "goosefs.user.client.cache.async.write.enabled".into(),
            "false".into(),
        );
        props.insert(
            "goosefs.user.client.cache.sequential.read.enabled".into(),
            "true".into(),
        );
        let cfg = parse_config(Some("127.0.0.1:9200"), Some(&props), None).unwrap();
        assert!(cfg.client_cache_enabled);
        assert_eq!(cfg.client_cache_page_size, 65536);
        assert_eq!(cfg.client_cache_dirs, vec!["/tmp/gfs-cache".to_string()]);
        assert!(!cfg.client_cache_async_write_enabled);
        assert!(cfg.client_cache_sequential_read_enabled);
    }
}
