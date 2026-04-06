# backup-wizard
Lightweight PostgreSQL backup job that runs inside Docker.

The container does three steps in order:

1. Runs `pg_dump -Fc`
2. Compresses the dump into `temp/`
3. Encrypts the compressed file into `backup/`

After encryption, the app uploads the final file to your S3-compatible bucket and removes expired files based on the retention setting.
If the configured bucket does not exist yet, the job attempts to create it before uploading.
After upload, the app verifies the remote object against the local encrypted file. Files up to 20 MB are verified end-to-end by downloading the full object and comparing hashes. Larger files are verified by hashing 10 random 100 KB byte ranges locally and comparing them with the same ranges downloaded from object storage.

The app creates these directories in the project root if they do not already exist:

- `temp/` for intermediary `.dump` and `.dump.gz` files
- `backup/` for the final encrypted `.dump.gz.enc` artifact

## Environment variables

Required:

- `DB_USER`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `OBJECT_STORAGE_URL`
- `OBJECT_STORAGE_ACCESS`
- `OBJECT_STORAGE_SECRET`
- `BUCKET_NAME`
- `ENCRYPTION_KEY`

Optional:

- `BACKUP_PREFIX` default: `DB_NAME`
- `OBJECT_STORAGE_PREFIX` default: `DB_NAME`
- `OBJECT_STORAGE_REGION` default: `auto`
- `OBJECT_STORAGE_ADDRESSING_STYLE` default: `path`
- `PG_DUMP_PATH` default: `pg_dump`
- `DATA_RETENTION_DAYS` default: `30`

## Build the image

```bash
docker build -t backup-wizard .
```

## Run the backup

If you already keep your credentials in `.env`, run:

```bash
docker run --rm --env-file .env -v "$PWD":/app backup-wizard
```

## Run with Docker Compose

For local testing, Compose bind-mounts the whole project root into `/app`, so `temp/`, `backup/`, `.env`, and the source files all stay in this repository on the host machine.

```bash
docker compose up --build
```

That runs the single backup job once. The generated files will appear under this project root.
The encrypted artifact is uploaded to the bucket, and files older than `DATA_RETENTION_DAYS` are removed from both the bucket prefix and the local `temp/` and `backup/` directories.

If you prefer explicit variables:

```bash
docker run --rm \
	-e DB_USER=postgres \
	-e DB_PASSWORD=secret \
	-e DB_HOST=host.docker.internal \
	-e DB_PORT=5432 \
	-e DB_NAME=mydb \
	-e OBJECT_STORAGE_URL=account-id.r2.cloudflarestorage.com \
	-e OBJECT_STORAGE_ACCESS=access-key \
	-e OBJECT_STORAGE_SECRET=secret-key \
	-e BUCKET_NAME=backup-bucket \
	-e ENCRYPTION_KEY=my-passphrase \
	-e DATA_RETENTION_DAYS=30 \
	-v "$PWD":/app \
	backup-wizard
```

On Linux, if your database is running on the host machine, you may need to add:

```bash
--add-host=host.docker.internal:host-gateway
```

That keeps all tooling inside the container while writing the generated files back into this project directory.
