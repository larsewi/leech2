# Terminology

This document defines the vocabulary used to describe leech2's data model.

## Schema (the shape)

### Field

A named, typed column declaration. Has a `name`, a `kind` (`TEXT` / `NUMBER` /
`BOOLEAN`), optional sentinels, and a `primary-key` flag. The schema concept —
what columns _exist_.

### Kind

The type tag of a field or cell, without payload: `TEXT`, `NUMBER`, `BOOLEAN`,
or `NULL`.

### Column

A position within a row. "Column 3" refers to the third field by position. Use
**column** when talking about _index_ (e.g. `changed_indices`); use **field**
when talking about _name and type_.

### Sentinel

A per-field CSV string that maps to a non-textual cell value. `null` substitutes
for SQL `NULL` on any non-primary-key field; on `BOOLEAN` fields, `true` and
`false` override the default `"true"` / `"false"` literals.

### Layout

The canonical column tuple of a table: primary-key columns first, then
subsidiary columns, each group sorted lexicographically by field name. A
**layout change** — adding, removing, or renaming a field — forces the patch to
carry a full state for that table instead of a delta. Reordering fields in
config does not register as a layout change.

### Injected field

A column added to all generated SQL but absent from the CSV source. Values are
configured statically in `[[injected-fields]]` or supplied at runtime via `lch
patch inject`. When any injected fields are present, full-state patches use
`DELETE ... WHERE` instead of `TRUNCATE` so co-tenants' rows are preserved.

## Data (the contents)

### Cell

A single typed value at one (row, column). The atom of the data model.

### Row

One horizontal slice of a table — every cell across every column. Use **row**
for positional or source-level contexts (a line in the CSV, a tuple in SQL
output); use **record** when talking about identity, lookup, or change tracking.

### Record

One row of a table, split into key and value halves:

```rust
Record { key: Vec<Cell>, value: Vec<Cell> }
```

The unit of data identity within a table.

### Key

The primary-key portion of a record — the cells that identify it.

### Value

The non-key portion of a record — its subsidiary cells. When a record exists,
its **value** is what the key maps to.

### Subsidiary

The adjective for non-key cells, columns, or fields. "Subsidiary cells" and
"value cells" name the same thing; prefer **subsidiary** as the adjective on
columns, fields, or indices, and **value** for the noun on the value side of a
record.

### Table

A schema (list of fields) plus a set of records keyed by primary key.

### State

A snapshot of one or more tables at one point in time.

## Changes (between states)

### Insert

A record present in the new state, absent before.

### Delete

A record present before, absent in the new state.

### Update

A record present in both, with at least one non-key cell changed. Carries `key`,
`old_value`, `new_value`, and the indices of changed columns.

### Delta

The change-set for _one_ table between two states: its inserts, deletes, and
updates.

### Rule

One of the 15 cases for merging a parent operation and a child operation on the
same primary key into a result. The full set is enumerated in
[DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md) and implemented in
`Delta::merge`.

## History (between blocks)

### Block

A committed delta-set across all tables, plus metadata (parent hash, timestamp).
The unit of history.

### Head

A pointer to the latest block.

### Patch

A consumer-facing payload of per-table changes. Each table is delivered as
either a delta or a full state — whichever is smaller, with layout changes
forcing full state. A single patch can mix both across its tables.

### Genesis

The zero-hash sentinel representing the start of the chain, before any blocks
exist. The implicit parent of block #1.

## Storage

### Work directory

The on-disk directory holding `HEAD`, `STATE`, `REPORTED`, config, and block
files.

### Hash

The content-addressed identifier of a block. Logs render the short form (first
seven characters).

### Reported

A pointer to the hash of the last block whose patch was successfully applied
downstream. Lives in the `REPORTED` file beside `HEAD`. New patches start from
**reported** by default; absence means the next patch is a full state.

### Truncation

The configurable pruning of old block files, controlled by the `[truncate]`
section (`max-blocks`, `max-age`, `remove-orphans`, `truncate-reported`). Runs
after each block creation. The chain stays valid because patches start from
**reported**, not from genesis.

## Roles

### Agent

An application that creates blocks and produces patches. Runs on the data-source
side, reading CSV files and building the block chain.

### Hub

An application that converts patches to SQL and applies them to the target
database.

### Feeder

An application that acts as both an agent and a hub.
