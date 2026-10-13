#!/usr/bin/env python3
"""Cross-platform preparation tools for Project Nexus."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import os
import shlex
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
REMOTE_JOB_DIRECTORY = "/home/turinguser/nexus-jobs"
JOB_JAR = PROJECT_ROOT / "target" / "JavaHadoop-1.0-SNAPSHOT.jar"
REMOTE_JAR = f"{REMOTE_JOB_DIRECTORY}/JavaHadoop-1.0-SNAPSHOT.jar"
JAVA_HOME = "/opt/temurin-17.0.20.1"
HADOOP_HOME = "/opt/hadoop-3.5.0"


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


def run_command(command: list[str], cwd: Path | None = None) -> None:
    try:
        subprocess.run(command, check=True, cwd=cwd)
    except FileNotFoundError as error:
        executable = command[0]
        raise RuntimeError(f"Command not found: {executable}.") from error
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"Command failed with exit code {error.returncode}: {' '.join(command)}") from error


def run_compose_command(arguments: list[str]) -> None:
    """Run Docker Compose from the repository root, independent of the caller's working directory."""
    run_command(["docker", "compose", *arguments], cwd=PROJECT_ROOT)


def run_remote(target: str, command: str) -> None:
    run_command(["ssh", target, command])


def manage_cluster(args: argparse.Namespace) -> None:
    if args.backend == "compose":
        if args.action == "start":
            run_compose_command(
                ["up", "--detach", "--build", "--wait", "--wait-timeout", "180", "hadoop"]
            )
        elif args.action == "stop":
            run_compose_command(["stop", "hadoop"])
        else:
            run_compose_command(["ps", "--all", "hadoop"])
        return

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


def build_project(_: argparse.Namespace) -> None:
    if os.name == "nt":
        command = ["cmd", "/c", str(PROJECT_ROOT / "mvnw.cmd"), "-DskipTests", "package"]
    else:
        command = ["sh", str(PROJECT_ROOT / "mvnw"), "-DskipTests", "package"]
    run_command(command)


def require_hdfs_path(value: str, label: str) -> str:
    if not value.startswith("/") or "\x00" in value or ".." in Path(value).parts:
        raise ValueError(f"{label} must be an absolute HDFS path without '..': {value}")
    return value.rstrip("/") or "/"


def remote_job_command(target: str, class_name: str, arguments: list[str], output_paths: list[str]) -> None:
    quoted_outputs = [shlex.quote(path) for path in output_paths]
    checks = " ".join(
        f"if hdfs dfs -test -e {path}; then echo 'Output already exists: {path}' >&2; exit 2; fi;"
        for path in quoted_outputs
    )
    quoted_arguments = " ".join(shlex.quote(argument) for argument in arguments)
    remote = (
        f"export JAVA_HOME={shlex.quote(JAVA_HOME)} HADOOP_HOME={shlex.quote(HADOOP_HOME)} "
        f"HADOOP_CONF_DIR={shlex.quote(REMOTE_CONFIG_DIRECTORY)} "
        f"HADOOP_MAPRED_HOME={shlex.quote(HADOOP_HOME)}; "
        f"export PATH=\"$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH\"; "
        f"{checks} hadoop jar {shlex.quote(REMOTE_JAR)} {shlex.quote(class_name)} {quoted_arguments}"
    )
    run_remote(target, remote)


def compose_path_exists(path: str) -> bool:
    """Check an HDFS path in the Compose cluster without treating a missing path as a command failure."""
    command = ["docker", "compose", "exec", "-T", "hadoop", "hdfs", "dfs", "-test", "-e", path]
    result = subprocess.run(command, cwd=PROJECT_ROOT, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    detail = (result.stderr or result.stdout).strip()
    raise RuntimeError(
        f"Could not check HDFS path {path!r} in the Compose cluster: "
        f"{detail or f'exit code {result.returncode}'}"
    )


def compose_job_command(class_name: str, arguments: list[str]) -> None:
    """Submit one MapReduce stage to the local Hadoop container."""
    run_compose_command(
        [
            "exec",
            "-T",
            "hadoop",
            "hadoop",
            "jar",
            "/workspace/target/JavaHadoop-1.0-SNAPSHOT.jar",
            class_name,
            *arguments,
        ]
    )


def run_job(args: argparse.Namespace) -> None:
    target = args.target
    steps: list[tuple[str, list[str], list[str]]] = []

    if args.analysis == "activity":
        telemetry = require_hdfs_path(args.telemetry, "--telemetry")
        work = require_hdfs_path(args.work, "--work")
        output = require_hdfs_path(args.output, "--output")
        if work == "/" or output == "/":
            raise ValueError("--work and --output must not be the HDFS root.")
        start_date = dt.date.fromisoformat(args.start_date)
        distinct = f"{work}/daily-active-players"
        windowed = f"{work}/distinct-activity-windows"
        steps = [
            ("fr.thesakyo.nexus.mapreduce.activity.DailyActivePlayersJob", [telemetry, distinct], [distinct]),
            ("fr.thesakyo.nexus.mapreduce.activity.ActivityMetricsJob",
             [distinct, windowed, output, start_date.isoformat()], [windowed, output]),
        ]
    elif args.analysis == "retention":
        players = require_hdfs_path(args.players, "--players")
        telemetry = require_hdfs_path(args.telemetry, "--telemetry")
        work = require_hdfs_path(args.work, "--work")
        output = require_hdfs_path(args.output, "--output")
        if work == "/" or output == "/":
            raise ValueError("--work and --output must not be the HDFS root.")
        start_date = dt.date.fromisoformat(args.start_date)
        as_of_date = dt.date.fromisoformat(args.as_of_date)
        if as_of_date < start_date:
            raise ValueError("--as-of-date must not precede --start-date.")
        if args.max_day is None or args.max_day < 0 or args.max_day > 90:
            raise ValueError("--max-day must be between 0 and 90.")
        joined = f"{work}/player-cohort-activity"
        steps = [
            ("fr.thesakyo.nexus.mapreduce.retention.PlayerCohortJoinJob", [players, telemetry, joined], [joined]),
            ("fr.thesakyo.nexus.mapreduce.retention.CohortRetentionMetricsJob",
             [joined, output, start_date.isoformat(), as_of_date.isoformat(), str(args.max_day)], [output]),
        ]
    else:
        players = require_hdfs_path(args.players, "--players")
        telemetry = require_hdfs_path(args.telemetry, "--telemetry")
        work = require_hdfs_path(args.work, "--work")
        output = require_hdfs_path(args.output, "--output")
        if work == "/" or output == "/":
            raise ValueError("--work and --output must not be the HDFS root.")
        joined = f"{work}/technical-incidents-joined"
        steps = [
            ("fr.thesakyo.nexus.mapreduce.technical.TechnicalIncidentsByProfileJob",
             [players, telemetry, joined], [joined]),
            ("fr.thesakyo.nexus.mapreduce.technical.TechnicalIncidentAggregationJob", [joined, output], [output]),
        ]

    planned_outputs = [path for _, _, outputs in steps for path in outputs]
    for index, first in enumerate(planned_outputs):
        for second in planned_outputs[index + 1:]:
            if first == second or first.startswith(second + "/") or second.startswith(first + "/"):
                raise ValueError(f"Pipeline output paths must be separate and non-overlapping: {first} and {second}")

    if not JOB_JAR.is_file():
        raise FileNotFoundError(f"Job JAR not found: {JOB_JAR}. Run 'python scripts/nexus.py build' first.")

    if args.backend == "compose":
        run_compose_command(["exec", "-T", "hadoop", "hdfs", "dfsadmin", "-report"])
        for path in planned_outputs:
            if compose_path_exists(path):
                raise ValueError(f"Output already exists: {path}")
        for class_name, arguments, _ in steps:
            compose_job_command(class_name, arguments)
        return

    run_remote(target, f"mkdir -p {shlex.quote(REMOTE_JOB_DIRECTORY)}")
    run_command(["scp", str(JOB_JAR), f"{target}:{REMOTE_JAR}"])
    for class_name, arguments, outputs in steps:
        remote_job_command(target, class_name, arguments, outputs)


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
    cluster_parser.add_argument(
        "--backend", choices=("ssh", "compose"), default="ssh", help="cluster access mode"
    )
    cluster_parser.add_argument("--target", default=DEFAULT_SSH_TARGET, help="SSH alias for the configured project server")
    cluster_parser.set_defaults(handler=manage_cluster)

    build_parser = commands.add_parser("build", help="package Java MapReduce jobs without running tests")
    build_parser.set_defaults(handler=build_project)

    job_parser = commands.add_parser("job", help="build is separate; submit a MapReduce analysis to Hadoop")
    job_parser.add_argument("analysis", choices=("activity", "retention", "technical"))
    job_parser.add_argument("--telemetry", required=True, help="HDFS path containing telemetry JSONL")
    job_parser.add_argument("--work", required=True, help="new HDFS path for intermediate job outputs")
    job_parser.add_argument("--output", required=True, help="new HDFS output path for final metrics")
    job_parser.add_argument("--start-date", help="first fully observed UTC date, required for activity windows")
    job_parser.add_argument("--players", help="HDFS path containing MongoDB players JSONL for technical analysis")
    job_parser.add_argument("--as-of-date", help="last fully observed UTC date, required for retention")
    job_parser.add_argument("--max-day", type=int, help="maximum Dn retention offset to compute (0..90)")
    job_parser.add_argument(
        "--backend", choices=("ssh", "compose"), default="ssh", help="job execution mode"
    )
    job_parser.add_argument("--target", default=DEFAULT_SSH_TARGET, help="SSH alias for the configured project server")
    job_parser.set_defaults(handler=run_job)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "job":
            if args.analysis == "technical" and not args.players:
                raise ValueError("The technical analysis requires --players with a MongoDB players JSONL HDFS path.")
            if args.analysis == "activity":
                if not args.start_date:
                    raise ValueError("The activity analysis requires --start-date to avoid partial WAU windows.")
                try:
                    dt.date.fromisoformat(args.start_date)
                except ValueError as error:
                    raise ValueError("--start-date must use YYYY-MM-DD.") from error
            if args.analysis == "retention":
                if not args.players:
                    raise ValueError("Retention requires --players with a MongoDB players JSONL HDFS path.")
                if not args.start_date or not args.as_of_date or args.max_day is None:
                    raise ValueError("Retention requires --start-date, --as-of-date, and --max-day.")
                try:
                    dt.date.fromisoformat(args.start_date)
                    dt.date.fromisoformat(args.as_of_date)
                except ValueError as error:
                    raise ValueError("Retention dates must use YYYY-MM-DD.") from error
                if args.max_day < 0 or args.max_day > 90:
                    raise ValueError("--max-day must be between 0 and 90.")
            if args.analysis in ("activity", "retention"):
                if not args.start_date:
                    raise ValueError(f"The {args.analysis} analysis requires --start-date.")
                try:
                    dt.date.fromisoformat(args.start_date)
                except ValueError as error:
                    raise ValueError("--start-date must use YYYY-MM-DD.") from error
        args.handler(args)
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
