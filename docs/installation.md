# Installation

There are several ways to install and run SafeQL, depending on your environment and how deeply you want to integrate it.

- **Docker** (recommended for trying SafeQL quickly)
- **From source** (for development and customization)

---

## 1. Docker (Recommended)

### 1.1 Run docker
The easiest way to try SafeQL is to run it from a ready-to-use Docker image.
This runs PostgreSQL 17 with pgvecto.rs preinstalled. The default username is postgres and the password is mysecretpassword.

```bash
docker run \
  --name safeql-demo \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -p 5432:5432 \
  -d geonholee/pgvecto-rs:pg17-v0.1.0
```

### 1.2 Load system
Then connect to PostgreSQL:

```bash
psql -h localhost -p 5432 -U postgres
```

Enable the SafeQL extension:
This loads the vector extension (needed for semantic similarity ranking in SafeQL) and adjusts the search path so SafeQL functions can resolve correctly.

```sql
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;

LOAD 'vectors';
SET search_path TO "$user", public, vectors;
```


---

## 2. From Source (Developer Mode)

This method is suitable if you:

- Want to modify SafeQL itself
- Run experiments on refinement behavior
- Integrate SafeQL tightly into your own PostgreSQL setup

### 2.1 Prerequisites

Ensure you have:

- PostgreSQL installed with `pg_config` available  
- Rust toolchain (`rustup`)  
- `pgrx` installed for your PostgreSQL version  

Example installation:

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install pgrx and initialize for your PostgreSQL version
cargo install cargo-pgrx
cargo pgrx init
```

### 2.2 Clone and Build

```bash
git clone https://github.com/Geonho-Lee/SafeQL.git
cd SafeQL

# Build and install SafeQL as a PostgreSQL extension
cargo pgrx install --release --sudo
```

This compiles SafeQL and installs its extension files into the system PostgreSQL directories.

### 2.3 Configure PostgreSQL

Load the SafeQL library:

```bash
psql -U postgres -c "ALTER SYSTEM SET shared_preload_libraries = 'vectors';"
psql -U postgres -c "SELECT pg_reload_conf();"
```

Then enable the extension:

```sql
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;
```

Restart PostgreSQL if required:

```bash
sudo systemctl restart postgresql.service
```

