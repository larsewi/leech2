# TODO

## Add validation function for block and patch
When we read something from disk, we don't know if it has been corrupted.

## Only use pager in CLI if output cannot fit terminal window

## Add macros for LCH_SUCCESS, LCH_FAILURE in FFI header
Reason being that we may want more return values in the future.

## Add host identifier to SQL query to show which host the data comes from

## There should be a way to not report records that are too big

## There should be a way to filter records. E.g. they may contain sensitive data
Find out how to do this without ruining the delta merging rules

## Investigate how to package leech2
