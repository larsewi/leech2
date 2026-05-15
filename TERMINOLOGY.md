# Terminology

This document defines the vocabulary used to describe leech2's data model. It is
the **target** terminology — some names in the current code differ; see [Mapping
to current code](#mapping-to-current-code) at the bottom.

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

The on-disk directory holding `HEAD`, `STATE`, config, and block files.

### Hash

The content-addressed identifier of a block. Logs render the short form (first
seven characters).

## Roles

### Agent

An application that creates blocks and produces patches. Runs on the data-source
side, reading CSV files and building the block chain.

### Hub

An application that converts patches to SQL and applies them to the target
database.

### Feeder

An application that acts as both an agent and a hub.
