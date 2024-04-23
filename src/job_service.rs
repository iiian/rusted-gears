use tokio_postgres::Client;

use crate::types::{RgCache, RgJob, RgMoveJob};

pub(crate) struct RgJobService {
    pub(crate) client: Client,
    pub(crate) cache: RgCache,
}

impl Iterator for RgJobService {
    type Item = RgJob;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((rg_job, _)) = self.cache.pending.pop() {
            Some(rg_job)
        } else {
            None
        }
    }
}

impl RgJobService {
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
            .subcache_mut(&new_job.queue)
            .insert(new_job.id(), new_job.clone());
        self.cache.pending.push(new_job.clone(), new_job.priority);

        Ok(new_job)
    }

    pub(crate) async fn get_jobs(&self) -> Vec<RgJob> {
        self.cache.clone_all()
    }

    pub(crate) async fn move_job(&mut self, move_job: RgMoveJob) -> Result<Vec<RgJob>, String> {
        if let Some(job) = self
            .cache
            .find_any(&move_job.clone().into())
            .map(|e| e.clone())
        {
            let from_queue = &job.queue;
            let to_queue = move_job.to_queue;
            let new_job = self
                .client
                .query(
                    "
                    UPDATE job
                    SET queue = $1, in_progress = 'f'
                    WHERE namespace_id = $2 AND job_name = $3
                    RETURNING *;
                    ",
                    &[&to_queue, &job.namespace_id, &job.job_name],
                )
                .await
                .map(|v| v.into_iter().map(|e|Into::<RgJob>::into(e)).collect::<Vec<_>>())
                .map_err(|err| format!("Database update failed: {}", err))?;

            let id = job.id();
            self.cache.subcache_mut(&from_queue).remove(&id);
            self.cache.subcache_mut(&to_queue).insert(id, job);

            Ok(new_job)
        } else {
            return Err("Unknown job id".to_string());
        }
    }
}
