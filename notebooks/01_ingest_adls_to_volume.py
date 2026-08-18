# Databricks notebook source
# MAGIC %pip install azure-storage-file-datalake --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("storage_account", "stmedallionsajad01", "Storage account")
dbutils.widgets.text("container",       "datalake",           "Container")
dbutils.widgets.text("sas_token",       "",                   "SAS token (sv=...)")

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account").strip()
CONTAINER       = dbutils.widgets.get("container").strip()
SAS_TOKEN       = dbutils.widgets.get("sas_token").strip().lstrip("?")

VOLUME_ROOT = "/Volumes/workspace/tlc_bronze/landing"

assert SAS_TOKEN, "Paste your SAS token into the widget at the top of the notebook."
print(f"Source: {STORAGE_ACCOUNT}/{CONTAINER}  →  Target: {VOLUME_ROOT}")

# COMMAND ----------

from azure.storage.filedatalake import DataLakeServiceClient

service = DataLakeServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
    credential=SAS_TOKEN
)

fs = service.get_file_system_client(CONTAINER)

# Prove the connection: list what ADF produced under raw/tlc/
paths = [p.name for p in fs.get_paths(path="raw/tlc", recursive=True) if not p.is_directory]
print(f"Found {len(paths)} files in the raw zone:")
for p in sorted(paths):
    print("  ", p)

# COMMAND ----------

import os

def download(remote_path: str, local_path: str) -> int:
    """Copy one ADLS file into the Unity Catalog Volume. Returns bytes written."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    data = fs.get_file_client(remote_path).download_file().readall()
    with open(local_path, "wb") as f:
        f.write(data)
    return len(data)

# COMMAND ----------

copied, total = [], 0
for remote in sorted(paths):
    local = f"{VOLUME_ROOT}/{remote}"          # mirror the lake's folder structure inside the Volume
    size = download(remote, local)
    total += size
    copied.append((remote, size))
    print(f"  ✓ {remote:70s} {size/1_048_576:8.1f} MB")

# COMMAND ----------

df = spark.read.parquet(f"{VOLUME_ROOT}/raw/tlc/yellow/")
print(f"Rows across all months: {df.count():,}")
df.printSchema()
display(df.limit(5))