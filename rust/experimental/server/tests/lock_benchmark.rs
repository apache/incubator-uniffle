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

#[cfg(test)]
mod test {
    use dashmap::DashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tokio::time::Instant;

    struct Buffer {
        idx: i64,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore]
    async fn lock_test() -> anyhow::Result<()> {
        // 100 requests(4000 insert in single batch)

        let concurrency = 1000;
        let loop_size = 4000;

        let buffer_map: Arc<DashMap<String, Arc<Mutex<Buffer>>>> = Arc::new(DashMap::new());

        let mut handlers = vec![];
        let timer = Instant::now();
        for _ in 0..concurrency {
            let buffer = buffer_map.clone();
            handlers.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                for k in 0..loop_size {
                    let buf = buffer
                        .entry(k.to_string())
                        .or_insert_with(|| Arc::new(Mutex::new(Buffer { idx: 0 })))
                        .clone();
                    let mut buf = buf.lock().await;
                    buf.idx += 1;
                }
            }));
        }

        for handler in handlers {
            let _ = handler.await;
        }

        println!(
            "concurrent tokio lock time cost: {} ms",
            timer.elapsed().as_millis() - 10
        );

        let buffer_map: Arc<DashMap<String, Arc<Mutex<Buffer>>>> = Arc::new(DashMap::new());

        let timer = Instant::now();
        for _ in 0..concurrency {
            let buffer = buffer_map.clone();
            for k in 0..loop_size {
                let buf = buffer
                    .entry(k.to_string())
                    .or_insert_with(|| Arc::new(Mutex::new(Buffer { idx: 0 })))
                    .clone();
                let mut buf = buf.lock().await;
                buf.idx += 1;
            }
        }

        println!(
            "sequence tokio lock time cost: {} ms",
            timer.elapsed().as_millis() - 10
        );

        let buffer_map: Arc<DashMap<String, Arc<std::sync::Mutex<Buffer>>>> =
            Arc::new(DashMap::new());

        let timer = Instant::now();
        for _ in 0..concurrency {
            let buffer = buffer_map.clone();
            for k in 0..loop_size {
                let buf = buffer
                    .entry(k.to_string())
                    .or_insert_with(|| Arc::new(std::sync::Mutex::new(Buffer { idx: 0 })))
                    .clone();
                let mut buf = buf.lock().unwrap();
                buf.idx += 1;
            }
        }

        println!(
            "sequence std lock time cost: {} ms",
            timer.elapsed().as_millis() - 10
        );

        let buffer_map: Arc<DashMap<String, Arc<std::sync::Mutex<Buffer>>>> =
            Arc::new(DashMap::new());

        let mut handlers = vec![];
        let timer = Instant::now();
        for _ in 0..concurrency {
            let buffer = buffer_map.clone();
            handlers.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                for k in 0..loop_size {
                    let buf = buffer
                        .entry(k.to_string())
                        .or_insert_with(|| Arc::new(std::sync::Mutex::new(Buffer { idx: 0 })))
                        .clone();
                    let mut buf = buf.lock().unwrap();
                    buf.idx += 1;
                }
            }));
        }

        for handler in handlers {
            let _ = handler.await;
        }

        println!(
            "concurrent std lock time cost: {} ms",
            timer.elapsed().as_millis() - 10
        );

        Ok(())
    }
}
