# /new-tool — Add a new utility tool

## When to use
When asked to create a new CLI tool in the `tools/` package.

## Steps

1. **Create the module** at `tools/<name>.py` following this structure:
   - Module docstring (one line)
   - Imports: `argparse`, `os`/`pathlib`, and paths from `paths.py`
   - Core logic in pure functions (not in `main()`)
   - `main()` function with `argparse.ArgumentParser`
   - `if __name__ == "__main__": main()` guard

2. **Use `paths.py` for all directory paths.** If the tool needs a new path:
   - Add it to `paths.py` with an env var override and sensible default
   - Import it in the tool module

3. **Add entry point** to `pyproject.toml` under `[project.scripts]`:
   ```
   tool-name = "tools.module_name:main"
   ```

4. **Keep dependencies minimal.** Pipeline and tools use stdlib only. If an external package is truly needed, discuss before adding to `pyproject.toml`.

5. **Verify** the tool runs:
   ```bash
   uv run python -m tools.<name> --help
   ```

## Style reference
See `tools/fix_extensions.py` or `tools/strip_tags.py` for the canonical pattern: argparse CLI, paths from `paths.py`, report output, dry-run by default where destructive.
