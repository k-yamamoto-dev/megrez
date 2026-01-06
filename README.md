# megrez

megrez is a CLI tool to detect data file formats and print schema or content.

## Installation

```bash
cargo install --path .
```

## Usage

```bash
megrez schema tests/fixtures/sample.csv
megrez cat tests/fixtures/sample.parquet --limit 5
```

You can override format detection when needed:

```bash
megrez schema --format parquet path/to/file
megrez cat --format avro path/to/file
```

Stdin is supported for JSON/CSV only (use `-` as the input path):

```bash
cat data.json | megrez schema -
cat data.csv | megrez cat -
```

## Notes

- JSON schema inference samples up to 1000 records and limits nesting depth to 8.
- ORC support is behind the `orc` feature flag. When the feature is disabled, ORC files are reported as not supported.
- Schema output includes a header row by default; disable with `--show-columns=false` and `--show-format-name=false`.
- Parquet detection checks both the header and footer magic bytes (`PAR1`).
- Stdin detection uses a small prefix buffer and does not perform Parquet footer checks.

## Supported Formats

- JSON (newline-delimited JSON and JSON array)
- CSV
- Parquet
- Avro
- ORC (optional, feature: `orc`)

## License

MIT. See `LICENSE`.
