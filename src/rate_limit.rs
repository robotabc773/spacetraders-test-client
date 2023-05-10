// Based on https://gist.github.com/Jules-Bertholet/7bf734b3593e8f9831ef279246358b12

use std::time::Duration;

use reqwest::{Request, Response};
use task_local_extensions::Extensions;
use tokio::{
    sync::{Mutex, Semaphore, SemaphorePermit},
    time::sleep,
};

/// The current documented burst limit.
static BURST_LIMIT: u16 = 10;

/// The current documented per-second limit.
static RATE_LIMIT: u16 = 2;

/// A permit must be acquired from this pool before any API request.
static REQUEST_SEMAPHORE: Semaphore = Semaphore::const_new(BURST_LIMIT as usize);

struct ReturnPermit;
impl ReturnPermit {
    /// Wait for the appropriate amount of time (calculated from the rate limit), then returns a permit to the pool.
    async fn return_permit(&mut self, permit: SemaphorePermit<'_>) {
        sleep(Duration::from_millis(
            1000_u16.saturating_div(RATE_LIMIT).into(),
        ))
        .await;

        drop(permit);
    }
}

/// This mutex must be locked before a permit is returned to the pool.
/// Locking ensures that waiting periods are sequential.
static RETURN: Mutex<ReturnPermit> = Mutex::const_new(ReturnPermit);

/// Middleware to enforce rate-limiting for the SpaceTraders API.
#[derive(Default)]
pub struct RateLimitMiddleware;

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for RateLimitMiddleware {
    async fn handle(
        &self,
        reqest: Request,
        extensions: &mut Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        // Acquire a permit, yield if burst limit attained.
        let permit = REQUEST_SEMAPHORE.acquire().await.unwrap();
        let result = next.run(reqest, extensions).await;

        // Return permit to the pool after an appropriate timeout.
        tokio::spawn(async move {
            let mut return_permit = RETURN.lock().await;
            return_permit.return_permit(permit).await;
            drop(return_permit);
        });

        result
    }
}
