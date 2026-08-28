# Decision 0001: `tokio-postgres` storage driver

## Decision

Use `tokio-postgres` for the asynchronous PostgreSQL adapter. The adapter uses
only parameterized SQL written in the repository; it does not use an ORM, query
builder, schema synchronizer, migration framework, or a Neon SDK.

## Why and alternatives

The gateway runs asynchronous inference requests and must support standard
PostgreSQL hosts without blocking Tokio worker threads. Hand-writing the wire
protocol or wrapping a synchronous driver would be less safe and sustainable.
`sqlx` adds a broader abstraction than required; Diesel and SeaORM conflict
with the SQL-source-of-truth rule; and a Neon SDK would make an optional host a
product dependency.

## Scope and replacement

The used surface is connection management, transactions, parameterized queries,
and row decoding. If the driver becomes unsuitable, replace this one adapter
while preserving `ConfigurationRepository`; the domain and application crates
do not depend on it.

## Supply-chain posture

The selected release is lockfile-pinned by Cargo and is dual-licensed
`MIT OR Apache-2.0`. The resolved normal dependency tree currently contains 108
packages including the driver; advisory status and updates are reviewed in the
release gate. The driver is limited to the PostgreSQL adapter and has no effect
on the portable SQLite mode or on agent-facing contracts.
