#![allow(clippy::expect_used)]

mod rate_limit;

use crate::rate_limit::RateLimitMiddleware;

use std::{
    env,
    process,
    sync::Arc
};

use inquire::Select;
use strum::{EnumIter, IntoEnumIterator, Display};
use once_cell::sync::Lazy;
use spacedust::apis::agents_api::get_my_agent;
use spacedust::apis::configuration::Configuration;
use spacedust::apis::systems_api::get_systems_all;
use spacedust::models::System;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, QueryBuilder};
use reqwest_middleware::{Middleware, ClientWithMiddleware};
use tokio::sync::OnceCell;

fn setup_dotenv() {
    if dotenvy::dotenv().is_err() {
        eprintln!(".env file expected");
        process::exit(1);
    }
}

/// [`Configuration`] object for use in all API calls.
/// Sets API key and manages rate limit.
static CONFIGURATION: Lazy<Configuration> = Lazy::new(|| {
    let Ok(token) = env::var("TOKEN") else {
        eprintln!("TOKEN environment variable expected");
        process::exit(1);
    };

    let mut configuration = Configuration::new();
    configuration.bearer_access_token = Some(token);
    let middleware: Box<[Arc<dyn Middleware>]> = Box::new([Arc::new(RateLimitMiddleware)]);
    configuration.client = ClientWithMiddleware::new(reqwest::Client::new(), middleware);
    configuration
});

static DB_POOL: OnceCell<Pool<Postgres>> = OnceCell::const_new();
async fn get_global_db_pool() -> &'static Pool<Postgres> {
    DB_POOL.get_or_init(|| async {
        let Ok(database_url) = env::var("DATABASE_URL") else {
            eprintln!("DATABASE_URL environment variable expected");
            process::exit(1);
        };
        let Ok(pool) = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await 
        else {
            eprintln!("Database connection failed");
            process::exit(1);
        };
        pool
    }).await
}

const BIND_LIMIT: usize = 65535;

async fn create_systems_table (systems : &[System]) {
    println!("Creating systems table");

    sqlx::query("DROP TABLE IF EXISTS systems").execute(get_global_db_pool().await).await.expect("Delete systems table if it exists");

    sqlx::query("CREATE TABLE systems (
                symbol              text,
                sector_symbol       text,
                type                text,
                x                   int,
                y                   int,
                factions            text[]
            )")
        .execute(get_global_db_pool().await)
        .await
        .expect("Create systems table");
    
    let mut transaction = get_global_db_pool().await.begin().await.expect("Start insertion transaction");

    for systems_chunk in systems.chunks(BIND_LIMIT / 6) {
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO systems(symbol, sector_symbol, type, x, y, factions) "
            );
        query_builder.push_values(systems_chunk, |mut b, system| {
            b.push_bind(system.symbol.clone())
                .push_bind(system.sector_symbol.clone())
                .push_bind(system.r#type.to_string())
                .push_bind(system.x)
                .push_bind(system.y)
                .push_bind(system.factions.clone().into_iter().map(|x| x.symbol).collect::<Vec<String>>());
        });
        query_builder.build().execute(&mut transaction).await.expect("Insert into systems table");
    }

    transaction.commit().await.expect("Commit insertion transaction");
}

async fn create_waypoints_table (systems : &[System]) {
    println!("Creating waypoints table");

    sqlx::query("DROP TABLE IF EXISTS waypoints").execute(get_global_db_pool().await).await.expect("Delete waypoints table if it exists");

    sqlx::query("CREATE TABLE waypoints (
                symbol              text,
                type                text,
                system_symbol       text,
                x                   int,
                y                   int,
                is_marketplace      boolean,
                is_shipyard         boolean
            )")
        .execute(get_global_db_pool().await)
        .await
        .expect("Create waypoints table");
    
    let mut transaction = get_global_db_pool().await.begin().await.expect("Start insertion transaction");

    for system in systems {
        if system.waypoints.is_empty() {
            continue;
        }
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO waypoints(symbol, type, system_symbol, x, y) "
            );
        query_builder.push_values(system.waypoints.iter(), |mut b, waypoint| {
            b.push_bind(waypoint.symbol.clone())
                .push_bind(waypoint.r#type.to_string())
                .push_bind(system.symbol.clone())
                .push_bind(waypoint.x)
                .push_bind(waypoint.y);
        });
        query_builder.build().execute(&mut transaction).await.expect("Insert into waypoints table");
    }

    transaction.commit().await.expect("Commit insertion transaction");
}

async fn ensure_systems_data () {

    let systems_exists = sqlx::query("SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'systems'")
        .execute(get_global_db_pool().await)
        .await
        .expect("Postgres test query")
        .rows_affected() > 0;

    let waypoints_exists = sqlx::query("SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'waypoints'")
        .execute(get_global_db_pool().await)
        .await
        .expect("Postgres test query")
        .rows_affected() > 0;

    //TODO: Change this to use systems.json only for the systems table
    //      and then use get_systems_waypoints for the waypoints table
    //      this will allow getting more thorough information about 
    //      orbitals, faction, traits, and chart.
    //      Challenges: pagination
    //                  reversing orbital reference? - 
    //                      I think I want waypoints to know what they
    //                      orbit and not the other way around
    //                  traits - do we keep all of them in an array
    //                           or just the currently relevant ones 
    //                           as bools
    //                  chart - do we keep it at all?
    //                  handle the possible error if the system isn't charted
    if !systems_exists || !waypoints_exists {
        let systems = get_systems_all(&CONFIGURATION).await.expect("Get all systems");
        create_systems_table(&systems).await;
        create_waypoints_table(&systems).await;
        
    }

}

#[derive(Debug, EnumIter, Display)]
enum MenuChoice {
    GetAgent,
    ListSystems,
    GetWaypoint,
    Exit
}

#[tokio::main]
async fn main() {
    //Setup
    setup_dotenv();
    ensure_systems_data().await;

    
    loop {
        match Select::new("Main Menu", MenuChoice::iter().collect()).prompt() {
            Err(err) => {
                println!("Prompt error! {err:#?}");
            }
            Ok(choice) => match choice {
                MenuChoice::GetAgent => {
                    if let Ok(res) = get_my_agent(&CONFIGURATION).await {
                        println!("{:#?}", *(res.data));
                    }

                    match get_my_agent(&CONFIGURATION).await {
                        Ok(res) => {
                            println!("{:#?}", *(res.data));
                        }
                        Err(err_res) => {
                            println!("{err_res:#?}");
                        }
                    }
                }
                MenuChoice::ListSystems => {
                    todo!("ListSystems");
                }
                MenuChoice::GetWaypoint => {
                    todo!("GetWaypoint");
                }
                MenuChoice::Exit => {
                    println!("Bye!");
                    break;
                }
            }
        }
    }
}
