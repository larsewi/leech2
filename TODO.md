# TODO

## There should be a way to not report records that are too big

## There should be a way to filter records. E.g. they may contain sensitive data
Find out how to do this without ruining the delta merging rules

## Add FFI log callback for C consumers
Expose a function to register a log callback so C consumers can capture
leech2 log messages in their own logging system instead of relying on stderr.

## Add validation function for block and patch (and patch?)
When we read something from disk, we don't know if it has been corrupted.

## Store all checksums as byte vectors and convert to string when ever printed
Maybe we should create a custom type and implement display?

## Allow header row in CSV files
Configured in config file (per table).
Check that fields match with table config, then skip row.

## Investigate how to package leech2
