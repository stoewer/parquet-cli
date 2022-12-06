> **Warning**
> This tool is still under development and might change at any point in time

parquet-cli
===========

A command line tool to analyze parquet files.

Build
-----

Prerequisites:
* Go 1.19 or higher
* Make

To build `parquet-cli` make sure the above requirements are met.
Then execute the following command from the root of the repository:

```bash
make build
```

This will create the binary `parquet-cli` in the root of the repository.

Run
---

You can use the parquet files in the [`example`](./example) directory to test `parquet-cli`:

```bash
./parquet-cli row-stats ./example/nested.parquet
```
