pub mod protobuf;
mod softql_parser;
mod softql_binder;
mod softql_resolver;
pub use softql_parser::*;

// From Postgres source: src/include/storage/lockdefs.h
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LockMode {
    NoLock = 0,                   // NoLock is not a lock mode, but a flag value meaning "don't get a lock"
    AccessShareLock = 1,          // SELECT
    RowShareLock = 2,             // SELECT FOR UPDATE/FOR SHARE
    RowExclusiveLock = 3,         // INSERT, UPDATE, DELETE
    ShareUpdateExclusiveLock = 4, // VACUUM (non-FULL), ANALYZE, CREATE INDEX CONCURRENTLY
    ShareLock = 5,                // CREATE INDEX (WITHOUT CONCURRENTLY)
    ShareRowExclusiveLock = 6,    // like EXCLUSIVE MODE, but allows ROW SHARE
    ExclusiveLock = 7,            // blocks ROW SHARE/SELECT...FOR UPDATE
    AccessExclusiveLock = 8,      // ALTER TABLE, DROP TABLE, VACUUM FULL, and unqualified LOCK TABLE
}

// From Postgres source: src/include/catalog/pg_trigger.h
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TriggerType {
    Row = 1,
    Before = 2,
    Insert = 4,
    Delete = 8,
    Update = 16,
    Truncate = 32,
    Instead = 64,
}
