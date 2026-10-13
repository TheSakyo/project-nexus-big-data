#!/usr/bin/env python3
"""Cross-platform preparation tools for Project Nexus."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORT = PROJECT_ROOT / "reports" / "local" / "dataset-inventory.csv"
CONFIG_DIRECTORY = PROJECT_ROOT / "config" / "hadoop"
CONFIG_FILES = (
    "core-site.xml",
    "hdfs-site.xml",
    "mapred-site.xml",
    "yarn-site.xml",
    "hadoop-env.sh",
    "yarn-env.sh",
    "log4j.properties",
)
DEFAULT_SSH_TARGET = "turing-server"
REMOTE_CONFIG_DIRECTORY = "/home/turinguser/hadoop-setup/conf"


def calculate_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def create_inventory(args: argparse.Namespace) -> None:
    root = Path(args.root).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"Dataset directory not found: {root}")

    report = Path(args.report).expanduser().resolve(strict=False)
    try:
        report.relative_to(root)
    except ValueError:
        pass
    else:
        raise ValueError("The inventory report must be saved outside the source directory.")

    report.parent.mkdir(parents=True, exist_ok=True)
    total_bytes = 0
    file_count = 0
    empty_count = 0
    sizes_by_extension: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    errors: list[str] = []

    def record_walk_error(error: OSError) -> None:
        errors.append(str(error))

    columns = ("Relative path", "Extension", "Size bytes", "Modified UTC", "Empty", "SHA-256")
    with report.open("w", encoding="utf-8-sig", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(columns)

        for directory, subdirectories, filenames in os.walk(
            root, topdown=True, followlinks=False, onerror=record_walk_error
        ):
            directory_path = Path(directory)
            subdirectories[:] = sorted(
                name for name in subdirectories if not (directory_path / name).is_symlink()
            )

            for filename in sorted(filenames):
                path = directory_path / filename
                if path.is_symlink():
                    continue

                try:
                    stat = path.stat()
                    extension = path.suffix.lower() or "[no extension]"
                    relative_path = path.relative_to(root).as_posix()
                    checksum = calculate_sha256(path) if args.sha256 else ""
                    modified_utc = dt.datetime.fromtimestamp(
                        stat.st_mtime, tz=dt.timezone.utc
                    ).isoformat()
                except OSError as error:
                    errors.append(f"{path}: {error}")
                    continue

                writer.writerow(
                    (relative_path, extension, stat.st_size, modified_utc, stat.st_size == 0, checksum)
                )
                file_count += 1
                total_bytes += stat.st_size
                if stat.st_size == 0:
                    empty_count += 1
                sizes_by_extension[extension][0] += 1
                sizes_by_extension[extension][1] += stat.st_size

    print(f"Source directory: {root}")
    print(f"Files: {file_count}")
    print(f"Total size: {total_bytes / (1024 ** 3):.2f} GiB ({total_bytes} bytes)")
    print(f"Empty files: {empty_count}")
    print(f"Report: {report}")
    print("Files by extension:")
    for extension, (count, size) in sorted(sizes_by_extension.items()):
        print(f"  {extension}: {count} file(s), {size / (1024 ** 3):.3f} GiB")

    if errors:
        print(f"Warning: {len(errors)} error(s) occurred during inventory:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        raise RuntimeError("The inventory is incomplete. Resolve the errors before using the report.")


def run_command(command: list[str]) -> None:
    try:
        subprocess.run(command, check=True)
    except FileNotFoundError as error:
        executable = command[0]
        raise RuntimeError(
            f"Command not found: {executable}. Python and the OpenSSH client (ssh/scp) must be available."
        ) from error
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"Command failed with exit code {error.returncode}: {' '.join(command)}") from error


def run_remote(target: str, command: str) -> None:
    run_command(["ssh", target, command])


def manage_cluster(args: argparse.Namespace) -> None:
    target = args.target

    if args.action == "start":
        config_paths = [CONFIG_DIRECTORY / name for name in CONFIG_FILES]
        missing = [str(path) for path in config_paths if not path.is_file()]
        if missing:
            raise FileNotFoundError("Missing configuration files: " + ", ".join(missing))

        run_remote(target, f"mkdir -p {REMOTE_CONFIG_DIRECTORY}")
        for path in config_paths:
            destination = f"{target}:{REMOTE_CONFIG_DIRECTORY}/"
            run_command(["scp", str(path), destination])
        remote_action = (
            "hdfs --daemon start namenode && "
            "hdfs --daemon start datanode && "
            "yarn --daemon start resourcemanager && "
            "yarn --daemon start nodemanager"
        )
    elif args.action == "stop":
        remote_action = (
            "yarn --daemon stop nodemanager; "
            "yarn --daemon stop resourcemanager; "
            "hdfs --daemon stop datanode; "
            "hdfs --daemon stop namenode"
        )
    else:
        remote_action = 'jps; echo "--- HDFS ---"; hdfs dfsadmin -report; echo "--- YARN ---"; yarn node -list'

    environment = (
        "export JAVA_HOME=/opt/temurin-17.0.20.1 "
        "HADOOP_HOME=/opt/hadoop-3.5.0 "
        f"HADOOP_CONF_DIR={REMOTE_CONFIG_DIRECTORY} "
        "HADOOP_MAPRED_HOME=/opt/hadoop-3.5.0; "
        'export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"; '
    )
    run_remote(target, environment + remote_action)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cross-platform preparation tools for Project Nexus.")
    commands = parser.add_subparsers(dest="command", required=True)

    inventory_parser = commands.add_parser("inventory", help="list files without changing the source data")
    inventory_parser.add_argument("--root", required=True, help="directory containing the dataset")
    inventory_parser.add_argument("--report", default=str(DEFAULT_REPORT), help="CSV report path outside the source data")
    inventory_parser.add_argument("--sha256", action="store_true", help="calculate a SHA-256 hash for every file")
    inventory_parser.set_defaults(handler=create_inventory)

    cluster_parser = commands.add_parser("cluster", help="start, stop, or check the Hadoop cluster")
    cluster_parser.add_argument("action", choices=("status", "start", "stop"))
    cluster_parser.add_argument("--target", default=DEFAULT_SSH_TARGET, help="SSH alias for the configured project server")
    cluster_parser.set_defaults(handler=manage_cluster)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.handler(args)
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
