use tokio_postgres::Client;

use crate::types::{RgCache, RgJob, RgMoveJob};

pub(crate) struct JobService {
    pub(crate) client: Client,
    pub(crate) cache: RgCache,
}

impl JobService {
    pub fn new(client: Client, cache: RgCache) -> Self {
        Self { client, cache }
    }

    pub async fn create_job(&mut self, job: RgJob) -> Result<RgJob, &'static str> {
        if self.cache.contains(&job) {
            return Err("Job already in cache");
        }

        let new_job = self
            .client
            .query_one(
                "
            INSERT INTO job (
                namespace_id, job_name, job_id, priority, queue
            ) VALUES (
                $1,$2,$3,$4,$5
            )
            RETURNING *;
            ",
                &[
                    &job.namespace_id.as_str(),
                    &job.job_name.as_str(),
                    &job.job_id.as_str(),
                    &job.priority,
                    &job.queue.as_str(),
                ],
            )
            .await
            .map(|v| Into::<RgJob>::into(v))
            .map_err(|_| "Database insert failed")?;

        self.cache
            .subcache_mut(&job.queue)
            .insert(job.id(), job.clone());
        self.cache.pending.push(job.clone(), job.priority);
        println!("Job added to cache");

        Ok(new_job)
    }

    pub(crate) async fn get_jobs(&self) -> Vec<RgJob> {
        self.cache.clone_of_all()
    }

    pub(crate) async fn move_job(&mut self, move_job: RgMoveJob) -> Result<RgJob, &'static str> {
        if let Some(job) = self
            .cache
            .find_any(&move_job.clone().into())
            .map(|e| e.clone())
        {
            let from_queue = &job.queue;
            let to_queue = move_job.to_queue;
            let new_job = self
                .client
                .query_one(
                    "
                    UPDATE job
                    SET queue = $1, in_progress = 'f'
                    WHERE namespace_id = $2 AND job_name = $3
                    RETURNING *;
                    ",
                    &[&to_queue, &job.namespace_id, &job.job_name],
                )
                .await
                .map(|v| Into::<RgJob>::into(v))
                .map_err(|_| "Database update failed")?;

            self.cache.subcache_mut(&from_queue).remove(&job.id());

            Ok(new_job)
        } else {
            return Err("Unknown job id");
        }
    }
}
