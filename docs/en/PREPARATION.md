# Dataset-independent preparation

The tools below can be prepared while the dataset downloads. Do not run them against the source directory until the user confirms the download is complete.

## Reproducible inventory

From a terminal with Python 3.9 or later, at the repository root:

```sh
python scripts/nexus.py inventory --root 'D:\path\to\the\dataset'
```

On Linux or macOS, use `python3` if `python` is unavailable. The tool records relative paths, extensions, sizes, modification times, and empty files. It writes the CSV to `reports/local/`, which Git ignores, and refuses to write the report inside the source directory. The basic inventory does not read file contents.

To also calculate SHA-256 for every file:

```sh
python scripts/nexus.py inventory --root 'D:\path\to\the\dataset' --sha256
```

SHA-256 reads every byte and may take a while for tens of GiB; it is optional. An inventory with access errors returns a failure code and must not be used for capacity planning.

## Managing the preparation cluster

Python and the OpenSSH client (`ssh` and `scp`) must be available. The `turing-server` SSH alias can be overridden with `--target`, which must point to the same server configured for this project:

```sh
python scripts/nexus.py cluster status
python scripts/nexus.py cluster start
python scripts/nexus.py cluster stop
```

Starting synchronizes the version-controlled configuration and starts the services. It never formats HDFS. It assumes the NameNode has already been formatted and the SSH account can start Hadoop. `stop` stops services in reverse order; it does not delete HDFS data.

## After the download is confirmed

1. Generate the inventory and compare sources with the delivered manifest and catalog.
2. Inspect a representative sample of each format without modifying originals.
3. Choose a transfer method that avoids an unnecessary full duplicate on the server.
4. Check available storage and reserve headroom for `PROCESSED`, results, and temporary files.
5. Import into `RAW` with a batch manifest and a restartable command that does not overwrite an existing ingestion.
6. Define schemas and checks before writing the business Map and Reduce functions.

The assignment's 40 GiB estimate is not verified. Final capacity planning will use observed sizes, compression, and intermediate storage needs. The server has only about 848 MiB RAM; if useful jobs cannot run within that limit, another host or a suitable processing strategy will be needed, followed by a measured scale experiment.

## Work that depends on the data

Formats, columns, validation rules, activity timestamp, join keys, business analyses, and final MapReduce jobs must not be guessed. They will be decided from the catalog and delivered files. The repository does not yet contain `docs/DATA_CATALOG.md`.

The [data catalog template](DATA_CATALOG_TEMPLATE.md) is ready to fill in after source inspection.
