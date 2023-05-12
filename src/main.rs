#![allow(clippy::expect_used)]

mod rate_limit;
mod st_util;

use crate::rate_limit::RateLimitMiddleware;

use std::fmt::Debug;
use std::{
    env,
    process,
    sync::Arc
};

use inquire::{Select, Text};
use strum::{EnumIter, IntoEnumIterator, Display};
use once_cell::sync::Lazy;
use spacedust::apis::configuration::Configuration;
use spacedust::models::System;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, QueryBuilder};
use reqwest_middleware::{Middleware, ClientWithMiddleware};
use tokio::sync::OnceCell;

//----------------------------------------------------------------------
//                              SETUP
//----------------------------------------------------------------------

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
            b.push_bind(&system.symbol)
                .push_bind(&system.sector_symbol)
                .push_bind(system.r#type.to_string())
                .push_bind(system.x)
                .push_bind(system.y)
                .push_bind(system.factions.iter().map(|x| &*x.symbol).collect::<Vec<&str>>());
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
            b.push_bind(&waypoint.symbol)
                .push_bind(waypoint.r#type.to_string())
                .push_bind(&system.symbol)
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

    if !systems_exists || !waypoints_exists {
        let systems = spacedust::apis::systems_api::get_systems_all(&CONFIGURATION).await.expect("Get all systems");
        create_systems_table(&systems).await;
        create_waypoints_table(&systems).await;
        
    }

}


//----------------------------------------------------------------------
//                            UTILITY
//----------------------------------------------------------------------

fn prompt_waypoint_symbol() -> String {
    Text::new("Enter waypoint symbol").prompt().expect("Prompt error")
}

fn prompt_system_symbol() -> String {
    Text::new("Enter system symbol").prompt().expect("Prompt error")
}

async fn system_symbol_from_waypoint_symbol(waypoint_symbol: &str) -> String {
    let (system_symbol,): (String,) = sqlx::query_as("SELECT system_symbol FROM waypoints WHERE symbol = $1")
        .bind(waypoint_symbol)
        .fetch_one(get_global_db_pool().await)
        .await
        .expect("System symbol fetching");
    system_symbol
}

//----------------------------------------------------------------------
//                          MENU CHOICES
//----------------------------------------------------------------------

#[derive(Debug, EnumIter, Display)]
enum MenuChoice {
    GetAgent,
    ListContracts,
    ListShips,
    ListWaypoints,
    GetWaypoint,
    Exit
}

async fn get_agent() {
    if let Ok(res) = spacedust::apis::agents_api::get_my_agent(&CONFIGURATION).await {
        println!("{:#?}", *(res.data));
    }

    match spacedust::apis::agents_api::get_my_agent(&CONFIGURATION).await {
        Ok(res) => {
            println!("{:#?}", *(res.data));
        }
        Err(err_res) => {
            println!("{err_res:#?}");
        }
    }
}

async fn list_contracts() {
    match st_util::list_contracts().await {
        Ok(contracts) => {
            for contract in contracts {
                println!("{contract:#?}");
            }
        }
        Err(err) => println!("Error listing contracts: {err:#?}")
    }
}

async fn list_ships() {
    match st_util::list_ships().await {
        Ok(ships) => {
            for ship in ships {
                println!("{ship:#?}");
            }
        }
        Err(err) => println!("Error listing contracts: {err:#?}")
    }
}

//TODO: have this populate more of the database with whatever useful information
async fn list_waypoints() {
    let system_symbol = &prompt_system_symbol();

    match st_util::list_system_waypoints(system_symbol).await {
        Ok(waypoints) => {
            for waypoint in waypoints {
                println!("{waypoint:#?}");
            }
        }
        Err(err) => println!("Error listing waypoints: {err:#?}")
    }

}

async fn get_waypoint() {
    let waypoint_symbol = prompt_waypoint_symbol();
    let system_symbol = system_symbol_from_waypoint_symbol(&waypoint_symbol).await;

    match spacedust::apis::systems_api::get_waypoint(&CONFIGURATION, &system_symbol, &waypoint_symbol).await {
        Ok(res) => {
            println!("{:#?}", *(res.data));
        }
        Err(err_res) => {
            println!("{err_res:#?}");
        }
    }
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
                MenuChoice::GetAgent => get_agent().await,
                MenuChoice::ListContracts => list_contracts().await,
                MenuChoice::ListShips => list_ships().await,
                MenuChoice::ListWaypoints => list_waypoints().await,
                MenuChoice::GetWaypoint => get_waypoint().await,
                MenuChoice::Exit => {
                    println!("Bye!");
                    break;
                }
            }
        }
    }
}
