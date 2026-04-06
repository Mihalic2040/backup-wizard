#!/usr/bin/env python3
import gzip
import importlib
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
TEMP_DIR = BASE_DIR / "temp"
BACKUP_DIR = BASE_DIR / "backup"


def load_dotenv_file(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        os.environ.setdefault(key, value)


load_dotenv_file(BASE_DIR / ".env")


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def require_env_int(name: str) -> int:
    value = require_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer, got: {value}") from exc


def optional_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def optional_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer, got: {value}") from exc


@dataclass
class Config:
    db_user: str
    db_password: str
    db_host: str
    db_port: int
    db_name: str

    object_storage_url: str
    object_storage_access: str
    object_storage_secret: str
    object_storage_sign: str
    object_storage_pub_url: str
    bucket_name: str
    encryption_key: str
    pg_dump_path: str
    backup_prefix: str
    object_storage_prefix: str
    object_storage_region: str
    object_storage_addressing_style: str
    data_retention_days: int

    @classmethod
    def from_env(cls) -> "Config":
        db_name = require_env("DB_NAME")
        return cls(
            db_user=require_env("DB_USER"),
            db_password=require_env("DB_PASSWORD"),
            db_host=require_env("DB_HOST"),
            db_port=require_env_int("DB_PORT"),
            db_name=db_name,
            object_storage_url=require_env("OBJECT_STORAGE_URL"),
            object_storage_access=require_env("OBJECT_STORAGE_ACCESS"),
            object_storage_secret=require_env("OBJECT_STORAGE_SECRET"),
            object_storage_sign=require_env("OBJECT_STORAGE_SIGN"),
            object_storage_pub_url=require_env("OBJECT_STORAGE_PUB_URL"),
            bucket_name=require_env("BUCKET_NAME"),
            encryption_key=require_env("ENCRYPTION_KEY"),
            pg_dump_path=optional_env("PG_DUMP_PATH", "pg_dump"),
            backup_prefix=optional_env("BACKUP_PREFIX", db_name),
            object_storage_prefix=optional_env("OBJECT_STORAGE_PREFIX", db_name),
            object_storage_region=optional_env("OBJECT_STORAGE_REGION", "auto"),
            object_storage_addressing_style=optional_env("OBJECT_STORAGE_ADDRESSING_STYLE", "path"),
            data_retention_days=optional_env_int("DATA_RETENTION_DAYS", 30),
        )


@dataclass
class BackupPaths:
    dump_file: Path
    compressed_file: Path
    encrypted_file: Path


def ensure_directories() -> None:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def build_backup_paths(prefix: str) -> BackupPaths:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_name = f"{prefix}_{timestamp}"
    return BackupPaths(
        dump_file=TEMP_DIR / f"{base_name}.dump",
        compressed_file=TEMP_DIR / f"{base_name}.dump.gz",
        encrypted_file=BACKUP_DIR / f"{base_name}.dump.gz.enc",
    )


def run_command(command: list[str], env: dict[str, str] | None = None) -> None:
    try:
        subprocess.run(command, check=True, env=env, capture_output=True, text=True)
    except FileNotFoundError as exc:
        executable = command[0]
        raise RuntimeError(f"Required command not found: {executable}") from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else ""
        stdout = exc.stdout.strip() if exc.stdout else ""
        details = stderr or stdout or f"Command exited with status {exc.returncode}"
        raise RuntimeError(f"Command failed: {' '.join(command)}\n{details}") from exc


def normalize_endpoint_url(endpoint: str) -> str:
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    return f"https://{endpoint}"


def build_object_key(prefix: str, file_name: str) -> str:
    normalized_prefix = prefix.strip("/")
    if not normalized_prefix:
        return file_name
    return f"{normalized_prefix}/{file_name}"


def create_s3_client(config: Config):
    try:
        boto3 = importlib.import_module("boto3")
        botocore_config = importlib.import_module("botocore.config")
    except ImportError as exc:
        raise RuntimeError("Missing boto3 dependency in the runtime environment") from exc

    return boto3.client(
        "s3",
        endpoint_url=normalize_endpoint_url(config.object_storage_url),
        region_name=config.object_storage_region,
        aws_access_key_id=config.object_storage_access,
        aws_secret_access_key=config.object_storage_secret,
        config=botocore_config.Config(
            signature_version="s3v4",
            s3={"addressing_style": config.object_storage_addressing_style},
        ),
    )


def get_s3_error_code(exc: Exception) -> str | None:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    error = response.get("Error")
    if not isinstance(error, dict):
        return None
    code = error.get("Code")
    return str(code) if code is not None else None


def create_remote_bucket(client, config: Config) -> None:
    request = {"Bucket": config.bucket_name}
    if config.object_storage_region not in {"", "auto", "us-east-1"}:
        request["CreateBucketConfiguration"] = {"LocationConstraint": config.object_storage_region}
    client.create_bucket(**request)


def ensure_remote_bucket_exists(config: Config) -> None:
    client = create_s3_client(config)
    try:
        client.head_bucket(Bucket=config.bucket_name)
    except Exception as exc:
        error_code = get_s3_error_code(exc)
        if error_code not in {"404", "NoSuchBucket", "NotFound"}:
            raise RuntimeError(
                "Object storage bucket is not accessible. "
                f"bucket={config.bucket_name} endpoint={normalize_endpoint_url(config.object_storage_url)}"
            ) from exc

        try:
            create_remote_bucket(client, config)
        except Exception as create_exc:
            raise RuntimeError(
                "Object storage bucket does not exist and could not be created. "
                f"bucket={config.bucket_name} endpoint={normalize_endpoint_url(config.object_storage_url)}"
            ) from create_exc


def run_pg_dump(config: Config, output_path: Path) -> None:
    command = [
        config.pg_dump_path,
        "-Fc",
        "-h",
        config.db_host,
        "-p",
        str(config.db_port),
        "-U",
        config.db_user,
        "-d",
        config.db_name,
        "-f",
        str(output_path),
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = config.db_password
    run_command(command, env=env)


def compress_file(source_path: Path, output_path: Path) -> None:
    with source_path.open("rb") as source_file, gzip.open(output_path, "wb", compresslevel=9) as target_file:
        shutil.copyfileobj(source_file, target_file)


def encrypt_file(source_path: Path, output_path: Path, encryption_key: str) -> None:
    env = os.environ.copy()
    env["BACKUP_ENCRYPTION_KEY"] = encryption_key
    command = [
        "openssl",
        "enc",
        "-aes-256-cbc",
        "-salt",
        "-pbkdf2",
        "-in",
        str(source_path),
        "-out",
        str(output_path),
        "-pass",
        "env:BACKUP_ENCRYPTION_KEY",
    ]
    run_command(command, env=env)


def upload_backup(config: Config, encrypted_file: Path) -> str:
    object_key = build_object_key(config.object_storage_prefix, encrypted_file.name)
    try:
        client = create_s3_client(config)
        client.upload_file(str(encrypted_file), config.bucket_name, object_key)
    except Exception as exc:
        raise RuntimeError(f"Failed to upload backup to object storage: {exc}") from exc
    return object_key


def cleanup_local_files(retention_days: int, protected_paths: set[Path] | None = None) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    protected_paths = protected_paths or set()
    for directory in (TEMP_DIR, BACKUP_DIR):
        if not directory.exists():
            continue

        for file_path in directory.iterdir():
            if not file_path.is_file():
                continue
            if file_path in protected_paths:
                continue

            modified_at = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
            if modified_at < cutoff:
                file_path.unlink(missing_ok=True)


def cleanup_remote_files(config: Config, protected_key: str | None = None) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=config.data_retention_days)
    prefix = config.object_storage_prefix.strip("/")
    list_prefix = f"{prefix}/" if prefix else ""
    continuation_token = None

    try:
        client = create_s3_client(config)
        while True:
            request = {"Bucket": config.bucket_name, "Prefix": list_prefix}
            if continuation_token is not None:
                request["ContinuationToken"] = continuation_token

            response = client.list_objects_v2(**request)
            expired_objects = [
                {"Key": entry["Key"]}
                for entry in response.get("Contents", [])
                if entry["LastModified"] < cutoff and entry["Key"] != protected_key
            ]

            if expired_objects:
                client.delete_objects(Bucket=config.bucket_name, Delete={"Objects": expired_objects})

            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")
    except Exception as exc:
        raise RuntimeError(f"Failed to clean up expired object storage backups: {exc}") from exc


def main() -> int:
    try:
        config = Config.from_env()
        ensure_directories()
        backup_paths = build_backup_paths(config.backup_prefix)
        ensure_remote_bucket_exists(config)

        run_pg_dump(config, backup_paths.dump_file)
        compress_file(backup_paths.dump_file, backup_paths.compressed_file)
        encrypt_file(backup_paths.compressed_file, backup_paths.encrypted_file, config.encryption_key)
        object_key = upload_backup(config, backup_paths.encrypted_file)
        cleanup_local_files(
            config.data_retention_days,
            protected_paths={
                backup_paths.dump_file,
                backup_paths.compressed_file,
                backup_paths.encrypted_file,
            },
        )
        cleanup_remote_files(config, protected_key=object_key)
    except (RuntimeError, ValueError) as err:
        print(f"Backup failed: {err}", file=sys.stderr)
        return 1

    print("Backup completed successfully.")
    print(f"Dump file: {backup_paths.dump_file}")
    print(f"Compressed file: {backup_paths.compressed_file}")
    print(f"Encrypted backup: {backup_paths.encrypted_file}")
    print(f"Uploaded object: s3://{config.bucket_name}/{object_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
