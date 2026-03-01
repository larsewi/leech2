# Merging Deltas

## Background

Leech2 tracks changes to CSV-backed tables as **deltas**. Each delta records
which rows were inserted, deleted, or updated in a table between two states.
When multiple blocks of changes need to be combined (e.g. replaying history),
their deltas are **merged** by examining what happened to each primary key
across both blocks.

We call the two blocks being merged **Parent** (the earlier block) and
**Current** (the later block). The merge produces a single **Result** delta that
represents the combined effect of both.

### Notation

- `insert(key, val)` — a row with the given value was created.
- `delete(key, val)` — a row with the given value was removed.
- `update(key, old → new)` — a row's value changed from `old` to `new`.

## Running example

Imagine a `users` table with primary key `id` and a `name` column:

| id  | name    |
|-----|---------|
| 1   | Alice   |
| 2   | Bob     |

Throughout this document we will show how different sequences of operations on
this table get merged.

---

## The 15 merging rules

### Rules 1-4, 8, 12 — Key only in one block

When a key appears in only one of the two blocks, there is no conflict. The
operation passes through to the result unchanged.

| Rule | Parent                   | Current                  | Result                   |
|------|--------------------------|--------------------------|--------------------------|
|   1  |                          | `insert(key, val)`       | `insert(key, val)`       |
|   2  |                          | `delete(key, val)`       | `delete(key, val)`       |
|   3  |                          | `update(key, old → new)` | `update(key, old → new)` |
|   4  | `insert(key, val)`       |                          | `insert(key, val)`       |
|   8  | `delete(key, val)`       |                          | `delete(key, val)`       |
|  12  | `update(key, old → new)` |                          | `update(key, old → new)` |

**Example (Rule 1):** Parent has no changes for key `3`. Current inserts a new
user `(3, Charlie)`. Result: `insert(3, Charlie)`.

**Example (Rule 4):** Parent inserts `(3, Charlie)`. Current has no further
changes for key `3`. Result: `insert(3, Charlie)`.

---

### Rules 5, 10, 11, 13 — Unresolvable conflicts

These rules represent logically impossible combinations. They always produce an
error regardless of the values involved (shown as `X` meaning "don't care").

| Rule | Parent               | Current              | Result       |
|------|----------------------|----------------------|--------------|
|   5  | `insert(key, X)`     | `insert(key, X)`     | `error(key)` |
|  10  | `delete(key, X)`     | `delete(key, X)`     | `error(key)` |
|  11  | `delete(key, X)`     | `update(key, X → X)` | `error(key)` |
|  13  | `update(key, X → X)` | `insert(key, X)`     | `error(key)` |

**Rule 5 — Double insert:** You cannot insert the same key twice. If Parent
already inserted user `3`, Current cannot insert user `3` again.

**Rule 10 — Double delete:** You cannot delete a row that was already deleted.

**Rule 11 — Update after delete:** You cannot update a row that no longer
exists.

**Rule 13 — Insert after update:** If Parent updated a key, it must already
exist. Inserting it again in Current is a contradiction.

---

### Rule 6 — Insert then delete (cancels out)

| Rule | Parent           | Current          | Result |
|------|------------------|------------------|--------|
|   6  | `insert(key, X)` | `delete(key, X)` |        |

An insert followed by a delete always cancels out, regardless of values. The
values may differ because intermediate operations (such as updates in blocks
between Parent and Current) can change the value between the insert and the
delete.

**Example:** Parent inserts `(3, Charlie)`. Later, Charlie's name is updated to
`Charles` (in an intermediate block that has already been squashed). Current
then deletes `(3, Charles)`. Even though the values differ (`Charlie` vs
`Charles`), the net effect is: the row was added and then removed — so the
result is empty.

---

### Rule 7 — Insert then update

| Rule | Parent              | Current                 | Result              |
|------|---------------------|-------------------------|---------------------|
|   7  | `insert(key, val1)` | `update(key, X → val2)` | `insert(key, val2)` |

If a row was inserted and later updated, the combined effect is an insert with
the final value. The update's old value does not matter for the result.

**Example:** Parent inserts `(3, Charlie)`. Current updates key `3` from
`Charlie` to `Charles`. Result: `insert(3, Charles)` — from the perspective of
the merged result, the row simply appeared with the name `Charles`.

---

### Rules 9a, 9b — Delete then insert

| Rule | Parent              | Current             | Result                     |
|------|---------------------|---------------------|----------------------------|
|  9a  | `delete(key, val)`  | `insert(key, val)`  |                            |
|  9b  | `delete(key, val1)` | `insert(key, val2)` | `update(key, val1 → val2)` |

**Rule 9a:** If we delete a row and then re-insert it with the *same* value, the
two operations cancel out — the row is back to its original state.

**Example:** Parent deletes `(2, Bob)`. Current re-inserts `(2, Bob)`. Result:
nothing changed.

**Rule 9b:** If we delete a row and re-insert it with a *different* value, the
net effect is an update from the old value to the new value.

**Example:** Parent deletes `(2, Bob)`. Current inserts `(2, Robert)`. Result:
`update(2, Bob → Robert)` — from the outside, key `2` still exists but its
value changed.

---

### Rules 14a, 14b — Update then delete

| Rule | Parent                   | Current            | Result             |
|------|--------------------------|--------------------|--------------------|
|  14a | `update(key, old → new)` | `delete(key, new)` | `delete(key, old)` |
|  14b | `update(key, X → new)`   | `delete(key, val)` | `error(key)`       |

In rule 14b, `val ≠ new`.

**Rule 14a:** If we update a row and then delete it, and the delete's value
matches the update's new value, the result is a delete carrying the update's
**old** value. This is because the combined effect — from the perspective of the
state before Parent — is that the row with its original value was removed.

**Example:** Parent updates `(1, Alice)` to `(1, Alicia)`. Current deletes
`(1, Alicia)`. Result: `delete(1, Alice)` — the merged delta records that the
row with value `Alice` (the value before any of these changes) was deleted.

**Rule 14b:** If the delete's value does *not* match the update's new value,
this is a genuine conflict.

**Example:** Parent updates key `1` from `Alice` to `Alicia`. Current claims to
delete key `1` with value `Alice` (stale data). Result: error — the values are
inconsistent.

---

### Rule 15 — Update then update

| Rule | Parent                   | Current                  | Result                   |
|------|--------------------------|--------------------------|--------------------------|
|  15  | `update(key, old → X)`   | `update(key, X → new)`   | `update(key, old → new)` |

When two updates are stacked, the result is an update from the first update's
old value to the second update's new value. The intermediate value (`X`) does
not matter for the result.

**Example:** Parent updates `(1, Alice)` to `(1, Alicia)`. Current updates key
`1` from `Alicia` to `Ali`. Result: `update(1, Alice → Ali)`.

---

## Quick reference

| Rule | Parent   | Current   | Result              |
|------|----------|-----------|---------------------|
|   1  |          | `insert`  | `insert`            |
|   2  |          | `delete`  | `delete`            |
|   3  |          | `update`  | `update`            |
|   4  | `insert` |           | `insert`            |
|   5  | `insert` | `insert`  | **error**           |
|   6  | `insert` | `delete`  | *(cancels out)*     |
|   7  | `insert` | `update`  | `insert(new val)`   |
|   8  | `delete` |           | `delete`            |
|  9a  | `delete` | `insert=` | *(cancels out)*     |
|  9b  | `delete` | `insert≠` | `update(old → new)` |
|  10  | `delete` | `delete`  | **error**           |
|  11  | `delete` | `update`  | **error**           |
|  12  | `update` |           | `update`            |
|  13  | `update` | `insert`  | **error**           |
|  14a | `update` | `delete=` | `delete(old)`       |
|  14b | `update` | `delete≠` | **error**           |
|  15  | `update` | `update`  | `update(old → new)` |

`=` means values match, `≠` means values differ.
