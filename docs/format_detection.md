# Format Detection Notes

This document summarizes how `megrez` detects file formats, the order of checks, and the factors used for each format. The goal is to make the detection rules explicit and easy to reason about.

## Detection Order

1. Magic bytes (fast header inspection)
2. File extension (fallback)
3. Content sniffing (lightweight parse)

If detection is ambiguous or fails, `megrez` reports an unsupported format error and includes the detection attempts in the error message.
You can bypass detection with `--format` on the `schema` or `cat` command.

## Stdin Input

When the input path is `-`, `megrez` reads from stdin. In this mode:
- Only JSON and CSV are supported.
- Detection uses a small prefix buffer (64 KB) and does not check Parquet footers.
- JSON is chosen if the first non-whitespace byte is `{` or `[`; otherwise CSV is assumed.

## Format-Specific Factors

### JSON
- **Magic bytes**: none
- **Extension**: `.json`, `.jsonl`, `.ndjson`
- **Content sniff**: first non-whitespace byte is `{` or `[` (object or array)
- **Notes**: JSON arrays and newline-delimited JSON are both accepted.

### CSV
- **Magic bytes**: none
- **Extension**: `.csv`
- **Content sniff**: attempt to parse a first record with the CSV reader and ensure the record is non-empty
- **Notes**: The CSV parser uses default settings with headers enabled.

### Parquet
- **Magic bytes**: `PAR1` at the start of the file
- **Footer bytes**: `PAR1` at the end of the file (Parquet footer marker)
- **Extension**: `.parquet`
- **Content sniff**: not used (magic bytes or extension should match)

### Avro
- **Magic bytes**: `Obj\x01` at the start of an Avro container file
- **Extension**: `.avro`
- **Content sniff**: not used (magic bytes or extension should match)

### ORC (optional)
- **Magic bytes**: `ORC` at the start of the file
- **Extension**: `.orc`
- **Content sniff**: not used (magic bytes or extension should match)
- **Notes**: ORC reading is not implemented unless built with the `orc` feature.

## Practical Implications

- **Robustness**: Magic bytes provide the most reliable detection when present.
- **Extensions**: Extensions are accepted even if headers are missing or small files are truncated.
- **Ambiguity**: If neither magic bytes nor extension match, JSON is tried first via content sniffing, then CSV; failure yields a clear unsupported-format error.
