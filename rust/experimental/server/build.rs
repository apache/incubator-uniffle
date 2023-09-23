// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::path::Path;
use std::{env, fs};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // generate the uniffle code for grpc server
    let mut config = prost_build::Config::new();
    config.bytes(&["."]);

    tonic_build::configure()
        .build_server(true)
        .out_dir("src/proto")
        .compile_with_config(config, &["src/proto/uniffle.proto"], &["."])?;

    // rename the generated filename to uniffle.rs
    rename_file("src/proto/rss.common.rs", "src/proto/uniffle.rs");

    // only setup ld library path in debug mode
    let profile = std::env::var("PROFILE").unwrap();
    if profile == "debug" {
        setup_ld_library_path();
    }
    Ok(())
}

fn rename_file(file_path: impl AsRef<Path>, renamed_path: impl AsRef<Path>) {
    let f = file_path.as_ref();
    if !f.exists() || !f.is_file() {
        panic!("The file is missing or not a file.");
    }
    fs::rename(&f, renamed_path).expect("Errors on renaming file.");
}

fn setup_ld_library_path() {
    // java_home is required now to build and test
    let java_home = env::var("JAVA_HOME").expect("JAVA_HOME must be set");
    let possible_lib_paths = vec![
        format!("{java_home}/jre/lib/amd64/server/"),
        format!("{java_home}/lib/server"),
        format!("{java_home}/jre/lib/server"),
        format!("{java_home}/jre/lib/amd64/server"),
    ];
    let lib_jvm_path = possible_lib_paths
        .iter()
        .find(|&path| {
            let path = Path::new(&path);
            path.exists()
        })
        .expect("java_home is not valid");
    match env::consts::OS {
        "linux" => {
            let ld_path = env::var_os("LD_LIBRARY_PATH").unwrap_or("".parse().unwrap());
            let ld_path = format!("{}:{}", ld_path.to_str().unwrap(), lib_jvm_path);
            // this might be anti-pattern, but it works for our current setup
            println!("cargo:rustc-env=LD_LIBRARY_PATH={}", ld_path);
        }
        "macos" => {
            let ld_path = env::var_os("DYLD_LIBRARY_PATH").unwrap_or("".parse().unwrap());
            let ld_path = format!("{}:{}", ld_path.to_str().unwrap(), lib_jvm_path);
            // this might be anti-pattern, but it works for our current setup
            println!("cargo:rustc-env=DYLD_LIBRARY_PATH={}", ld_path);
        }
        _ => {
            // do nothing
        }
    }
}
