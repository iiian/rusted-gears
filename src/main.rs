use dotenv::dotenv;
use job_service::RgJobService;
use std::{error::Error, sync::Arc};
use tokio::sync::RwLock;
use types::RgJob;

use axum::{
    routing::{get, post, put},
    Router,
};
use tokio_postgres::{Client, NoTls};

use crate::types::{RgAppState, RgCache};

mod types;

mod routes;

mod job_service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().expect("missing .env");
    let client = get_ensured_db_client().await?;
    let app_state = init_app_state(client).await?;
    let app_state = Arc::new(app_state);
    let worker_app_state = app_state.clone();
    tokio::spawn(async move {
        while let Some(rg_job) = worker_app_state.job_service.write().await.next() {
            // todo: actually handle
            rg_job.
        }
    });

    println!("rusted-gears listening on port :2024");
    axum::serve(
        tokio::net::TcpListener::bind("0.0.0.0:2024").await?,
        Router::new()
            .route("/create", post(routes::create_job_route))
            .route("/get", get(routes::get_jobs))
            .route("/move", put(routes::move_job))
            .with_state(app_state),
    )
    .await?;

    Ok(())
}

async fn get_ensured_db_client() -> Result<Client, Box<dyn Error>> {
    let conn_str = format!(
        "host={} user={} password='{}'",
        std::env::var("POSTGRES_HOST")?,
        std::env::var("POSTGRES_USER")?,
        std::env::var("POSTGRES_PASS")?
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {}", e);
        }
    });

    // Check if the target database exists
    let is_some_rg_table = client
        .query("SELECT 1 FROM pg_database WHERE datname='rg'", &[])
        .await?;
    if is_some_rg_table.is_empty() {
        // Create the database if it does not exist
        client.execute("CREATE DATABASE rg", &[]).await?;
    }

    let conn_str = format!(
        "host={} user={} password='{}' dbname={}",
        std::env::var("POSTGRES_HOST")?,
        std::env::var("POSTGRES_USER")?,
        std::env::var("POSTGRES_PASS")?,
        "rg"
    );

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // ensure tables & triggers
    client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS job (
                namespace_id TEXT NOT NULL,
                job_name TEXT NOT NULL,
                job_id TEXT NOT NULL,
                priority INT NOT NULL,
                queue TEXT NOT NULL,
                in_progress BOOLEAN DEFAULT false,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (namespace_id, job_name)
            );

            CREATE OR REPLACE FUNCTION update_modified_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            DO $$
            BEGIN
                -- Check if the trigger already exists
                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'before_update_trigger') THEN
                    CREATE TRIGGER before_update_trigger
                    BEFORE UPDATE ON job
                    FOR EACH ROW EXECUTE PROCEDURE update_modified_column();
                END IF;
            END;
            $$;

            CREATE TABLE IF NOT EXISTS archive (
                namespace_id TEXT NOT NULL,
                job_name TEXT NOT NULL,
                job_id TEXT NOT NULL,
                priority INT NOT NULL,
                queue TEXT NOT NULL,
                completed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (namespace_id, job_name)
            );
            ",
        )
        .await?;

    Ok(client)
}

async fn init_app_state(client: Client) -> Result<RgAppState, Box<dyn Error>> {
    let results = client.query("SELECT * FROM job;", &[]).await?;
    let mut cache = RgCache::new();
    for row in results {
        let job: RgJob = row.into();
        cache.subcache_mut(&job.queue).insert(job.id(), job);
    }

    Ok(RgAppState::new(RwLock::new(RgJobService::new(client, cache))))
}
