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
