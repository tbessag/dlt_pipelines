import dlt
from dlt.sources.filesystem import filesystem, FileItemDict
import fsspec

import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('fsspec').setLevel(logging.DEBUG)
logging.getLogger('dlt').setLevel(logging.DEBUG)


# Configuration
host = "ftsplus.airbus.corp"
username = "xxx"
password = "xx*xx;xx+x"  # Raw password with semicolon
base_path = "/xxx-walter/master-data/dev/new"

# Method 1: Pre-create the filesystem (RECOMMENDED)
ftp_fs = fsspec.filesystem(
    "ftp",
    host=host,
    username=username,
    password=password
)

# Now use dlt with the pre-configured filesystem
ftp_files = filesystem(
    bucket_url=base_path,  # Just the path, no protocol
    file_glob="*.csv",
    files_per_page=10,
    credentials=ftp_fs  # Pass the filesystem object directly
)

pipeline = dlt.pipeline(
    pipeline_name="ftp_csv_pipeline",
    destination="duckdb",
    dataset_name="ftp_data"
)

load_info = pipeline.run(ftp_files)
print(load_info)

##

import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('fsspec').setLevel(logging.DEBUG)
logging.getLogger('dlt').setLevel(logging.DEBUG)


#########

import dlt
from dlt.sources.filesystem import filesystem
from urllib.parse import quote_plus

host = "ftsplus.airbus.corp"
username = "xxx"
password = "xx*xx;xx+x"

# Create storage_options dict (fsspec's native way)
storage_options = {
    "host": host,
    "username": username,
    "password": password
}

# Use simplefs:// protocol which bypasses URL parsing
bucket_url = f"simplefs:///xxx-walter/master-data/dev/new"

ftp_files = filesystem(
    bucket_url=bucket_url,
    file_glob="*.csv",
    credentials=storage_options
)


########

import dlt
import fsspec
import pandas as pd
from typing import Iterator

@dlt.resource(name="ftp_csv_files")
def ftp_csv_reader() -> Iterator[dict]:
    """Custom DLT resource that uses working fsspec code"""
    
    # Your WORKING fsspec code
    fs = fsspec.filesystem(
        "ftp",
        host="ftsplus.airbus.corp",
        username="xxx",
        password="xx*xx;xx+x"
    )
    
    # List files
    file_path = "/xxx-walter/master-data/dev/new"
    files = fs.glob(f"{file_path}/*.csv")
    
    # Process each file
    for file in files:
        with fs.open(file, 'r') as f:
            df = pd.read_csv(f)
            # Yield records
            for record in df.to_dict('records'):
                yield record

# Use the custom source
pipeline = dlt.pipeline(
    pipeline_name="ftp_csv_pipeline",
    destination="duckdb",
    dataset_name="ftp_data"
)

load_info = pipeline.run(ftp_csv_reader())
print(load_info)
