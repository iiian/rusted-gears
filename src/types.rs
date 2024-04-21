use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use tokio_postgres::Row;

use priority_queue::PriorityQueue;

use std::{collections::HashMap, hash::Hash};

use crate::job_service::JobService;

pub mod timestamp;

#[derive(Default, Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RgMoveJob {
    pub(crate) namespace_id: String,
    pub(crate) job_name: String,
    pub(crate) to_queue: String,
}

impl Into<RgJobId> for RgMoveJob {
    fn into(self) -> RgJobId {
        (self.namespace_id, self.job_name)
    }
}

#[derive(Default, Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RgJob {
    pub(crate) namespace_id: String,
    pub(crate) job_name: String,
    pub(crate) queue: String,
    pub(crate) priority: i32,
    pub(crate) job_id: String,
    pub(crate) updated_at: Option<timestamp::TimestampWithTimeZone>,
    pub(crate) in_progress: Option<bool>,
}

impl Hash for RgJob {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.namespace_id.hash(state);
        self.job_name.hash(state);
    }
}

impl RgJob {
    pub fn id(&self) -> (String, String) {
        (self.namespace_id.clone(), self.job_name.clone())
    }
}

impl Into<RgJob> for Row {
    fn into(self) -> RgJob {
        RgJob {
            job_id: self.get("job_id"),
            job_name: self.get("job_name"),
            namespace_id: self.get("namespace_id"),
            priority: self.get("priority"),
            queue: self.get("queue"),
            updated_at: self.get("updated_at"),
            in_progress: self.get("in_progress"),
        }
    }
}

pub type RgJobId = (String, String);

impl Into<RgJobId> for RgJob {
    fn into(self) -> RgJobId {
        self.id()
    }
}

#[derive(Clone)]
pub(crate) struct RgCache {
    pub(crate) inflight: HashMap<(String, String), RgJob>,
    pub(crate) fail: HashMap<(String, String), RgJob>,
    pub(crate) pending: PriorityQueue<RgJob, i32>,
}

impl RgCache {
    pub fn new() -> Self {
        Self {
            inflight: HashMap::new(),
            fail: HashMap::new(),
            pending: PriorityQueue::new(),
        }
    }

    pub fn contains(&self, job: &RgJob) -> bool {
        self.subcache(&job.queue).contains_key(&job.id())
    }

    pub fn find_any(&self, job: &RgJobId) -> Option<&RgJob> {
        if let Some(job) = self.inflight.get(job) {
            return Some(job);
        }
        if let Some(job) = self.fail.get(job) {
            return Some(job);
        }
        None
    }

    pub(crate) fn subcache(&self, queue: &str) -> &HashMap<(String, String), RgJob> {
        match queue {
            "INFLIGHT" => &self.inflight,
            "FAIL" => &self.fail,
            _ => panic!("unsupported"),
        }
    }

    #[inline]
    pub(crate) fn subcache_mut(&mut self, queue: &str) -> &mut HashMap<(String, String), RgJob> {
        match queue {
            "INFLIGHT" => &mut self.inflight,
            "FAIL" => &mut self.fail,
            _ => panic!("unsupported"),
        }
    }

    pub fn clone_of_all(&self) -> Vec<RgJob> {
        self.fail
            .values()
            .chain(self.inflight.values())
            .map(|e| e.clone())
            .collect()
    }
}

pub(crate) struct RgAppState {
    pub(crate) job_service: RwLock<JobService>,
}

impl RgAppState {
    pub fn new(job_service: RwLock<JobService>) -> Self {
        Self { job_service }
    }
}
