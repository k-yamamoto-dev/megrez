Contributing Guidelines

Thanks for your interest in improving megrez!

## Development Setup

- Install Rust (stable).
- Build: `cargo build`
- Test: `cargo test`
- Lint: `cargo clippy -- -D warnings`
- Format: `cargo fmt --all -- --check`

## Workflow

1. Fork and create a feature branch.
2. Make focused changes with clear commit messages.
3. Run tests and linters locally.
4. Open a PR and describe the intent and scope.
5. Address review feedback and keep the PR updated.

## Code Style

- Keep changes small and readable.
- Prefer explicit error handling over `unwrap`.
- Add or update tests when behavior changes.

## Reporting Issues

Please include:
- Steps to reproduce
- Expected vs actual behavior
- Sample input files (if relevant)
- `megrez --version` output
