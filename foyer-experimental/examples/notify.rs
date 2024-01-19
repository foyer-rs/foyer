use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use itertools::Itertools;
use tokio::sync::Notify;

async fn case() -> Duration {
    let n = Arc::new(Notify::default());
    let handles = (0..1000)
        .map(|_| {
            tokio::spawn({
                let n = n.clone();
                async move {
                    n.notified().await;
                }
            })
        })
        .collect_vec();

    tokio::time::sleep(Duration::from_millis(10)).await;

    let now = Instant::now();
    n.notify_waiters();
    let duration = now.elapsed();

    // println!("{:?}", duration);

    for handle in handles {
        handle.await.unwrap();
    }

    duration
}

#[tokio::main]
async fn main() {
    let mut min = Duration::from_secs(10);
    let mut max = Duration::from_nanos(0);
    for _ in 0..1000 {
        let d = case().await;
        if d > max {
            max = d;
        }
        if d < min {
            min = d
        }
    }
    println!("min: {:?}\nmax: {:?}", min, max);
}
