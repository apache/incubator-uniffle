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

use bytes::Bytes;
use crc32fast::Hasher;

use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const WORKER_IP: &str = "WORKER_IP";

pub fn get_local_ip() -> Result<IpAddr, std::io::Error> {
    let ip = std::env::var(WORKER_IP);
    if ip.is_ok() {
        Ok(ip.unwrap().parse().unwrap())
    } else {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
        socket.connect("8.8.8.8:80")?;
        let local_addr = socket.local_addr()?;
        Ok(local_addr.ip())
    }
}

pub fn gen_worker_uid(grpc_port: i32) -> String {
    let ip = get_local_ip().unwrap().to_string();
    format!("{}-{}", ip.clone(), grpc_port)
}

const LENGTH_PER_CRC: usize = 4 * 1024;
pub fn get_crc(bytes: &Bytes) -> i64 {
    let mut crc32 = Hasher::new();
    let offset = 0;
    let length = bytes.len();
    let crc_buffer = &bytes[offset..(offset + length)];

    for i in (0..length).step_by(LENGTH_PER_CRC) {
        let len = std::cmp::min(LENGTH_PER_CRC, length - i);
        let crc_slice = &crc_buffer[i..(i + len)];

        crc32.update(crc_slice);
    }

    crc32.finalize() as i64
}

pub fn current_timestamp_ms() -> u128 {
    let current_time = SystemTime::now();
    let timestamp = current_time.duration_since(UNIX_EPOCH).unwrap().as_millis();
    timestamp
}

pub fn current_timestamp_sec() -> u64 {
    let current_time = SystemTime::now();
    let timestamp = current_time.duration_since(UNIX_EPOCH).unwrap().as_secs();
    timestamp
}

pub struct ConcurrencyLimiter {
    remaining: Mutex<u32>,
}

pub struct Ticket<'a> {
    limiter: &'a ConcurrencyLimiter,
    id: u32,
}

impl ConcurrencyLimiter {
    fn new(size: u32) -> Self {
        ConcurrencyLimiter {
            remaining: Mutex::new(size),
        }
    }

    pub fn try_acquire(&self) -> Option<Ticket<'_>> {
        let mut remaining = self.remaining.lock().unwrap();
        if *remaining <= 0 {
            None
        } else {
            let id = *remaining;
            *remaining -= 1;
            Some(Ticket { limiter: self, id })
        }
    }
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        let mut lock = self.limiter.remaining.lock().unwrap();
        *lock += 1;
    }
}

#[cfg(test)]
mod test {
    use crate::util::{current_timestamp_sec, get_crc, ConcurrencyLimiter};
    use bytes::Bytes;

    #[test]
    fn ticket_test() {
        let limiter = ConcurrencyLimiter::new(2);
        {
            let ticket = limiter.try_acquire().unwrap();
            assert_eq!(2, ticket.id);

            let ticket = limiter.try_acquire().unwrap();
            assert_eq!(1, ticket.id);

            assert_eq!(true, limiter.try_acquire().is_none());
        }

        let ticket = limiter.try_acquire();
        assert_eq!(true, ticket.is_some());
    }

    #[test]
    fn time_test() {
        println!("{}", current_timestamp_sec());
    }

    #[test]
    fn crc_test() {
        let data = Bytes::from("hello world! hello china!");
        let crc_value = get_crc(&data);
        // This value is the same with java's implementation
        assert_eq!(3871485936, crc_value);
    }

    #[test]
    fn drop_test() {}
}
