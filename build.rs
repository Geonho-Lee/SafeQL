use std::env;
use std::path::PathBuf;
static LIBRARY_NAME: &str = "parsed_result_handler";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    println!("cargo:warning=🔧 build.rs is running!");

    let pg_config_path = env::var("PGRX_PG_CONFIG_PATH")
        .unwrap_or_else(|_| "pg_config".to_string());
    let includedir_server = String::from_utf8(
        std::process::Command::new(pg_config_path)
            .arg("--includedir-server")
            .output()?
            .stdout,
    )?;
    let includedir_server = includedir_server.trim();
    println!("cargo:warning=Postgres include path = {}", includedir_server);
    // let postgres_include_path = "/home/postgres/.pgrx/17.4/src/include/";

    let mut build = cc::Build::new();
    build
        .file("src/softql/postgres_deparse.c")
        .file("src/softql/parsed_result_handler.c")
        .file("src/softql/protobuf/pg_query.pb-c.c")
        .file("src/softql/protobuf-c/protobuf-c.c")
        .include("src/softql/")
        .include("src/softql/protobuf/")
        .include("src/softql/protobuf-c/")
        .include(includedir_server)
        .flag("-fPIC")
        .flag("-ftls-model=local-dynamic")
        .warnings(false);

    if env::var("PROFILE").unwrap() == "debug" || env::var("DEBUG").unwrap() == "1" {
        build.define("USE_ASSERT_CHECKING", None);
    }

    build.compile(LIBRARY_NAME);
    
    println!("cargo:warning=🔍 OUT_DIR = {}", out_dir.display());
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static={}", LIBRARY_NAME);
    println!("cargo:rustc-env=USER=postgres");
    
    Ok(())
}
