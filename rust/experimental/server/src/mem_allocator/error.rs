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

use std::{error, fmt};

#[derive(Debug)]
pub enum ProfError {
    MemProfilingNotEnabled, // when memory-prof feature is not enabled
    IoError(std::io::Error), /* io error occurred, such as read/write file error, create file err
                             * , etc */
    JemallocError(String), // jemalloc related error
    PathEncodingError(std::ffi::OsString), /* When temp files are in a non-unicode directory,
                            * OsString.into_string() will cause this error, */
    PathWithNulError(std::ffi::NulError),
}

pub type ProfResult<T> = Result<T, ProfError>;

impl fmt::Display for ProfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProfError::MemProfilingNotEnabled => {
                let message = r#"memory-prof was not enabled, please make sure the feature
memory-prof is enabled when building and set corresponding environment variables such as
_RJEM_MALLOC_CONF=prof:true"#;
                write!(f, "{}", message)
            }
            ProfError::IoError(e) => write!(f, "io error occurred {:?}", e),
            ProfError::JemallocError(e) => write!(f, "jemalloc error {}", e),
            ProfError::PathEncodingError(path) => {
                write!(f, "Dump target path {:?} is not unicode encoding", path)
            }
            ProfError::PathWithNulError(path) => {
                write!(f, "Dump target path {:?} contain an internal 0 byte", path)
            }
        }
    }
}

impl From<std::io::Error> for ProfError {
    fn from(e: std::io::Error) -> Self {
        ProfError::IoError(e)
    }
}

impl From<std::ffi::NulError> for ProfError {
    fn from(e: std::ffi::NulError) -> Self {
        ProfError::PathWithNulError(e)
    }
}

impl error::Error for ProfError {}
