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

// Allocators
#[cfg(all(unix, feature = "jemalloc"))]
#[path = "jemalloc.rs"]
mod imp;

#[cfg(not(all(unix, feature = "jemalloc")))]
#[path = "system_std.rs"]
mod imp;

use cap::Cap;

#[global_allocator]
pub static ALLOCATOR: Cap<imp::Allocator> = Cap::new(imp::allocator(), usize::max_value());

pub mod error;
pub type AllocStats = Vec<(&'static str, usize)>;

// when memory-prof feature is enabled, provide empty profiling functions
#[cfg(not(all(unix, feature = "memory-prof")))]
mod default;

#[cfg(not(all(unix, feature = "memory-prof")))]
pub use default::*;

// when memory-prof feature is enabled, provide jemalloc profiling functions
#[cfg(all(unix, feature = "memory-prof"))]
mod profiling;
#[cfg(all(unix, feature = "memory-prof"))]
pub use profiling::*;
