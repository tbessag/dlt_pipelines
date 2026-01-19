import dlt
from dlt.sources.filesystem import filesystem, read_csv

def load_satair_ftp_data():
    """
    Standard dlt pipeline to read CSVs from FTP and load to Postgres.
    Solution for Special Characters: 
    - User/Host/Port go in the URL.
    - Password goes in the credentials dict (passed as raw string).
    """
    
    pipeline = dlt.pipeline(
        pipeline_name="satair_ftp_pipeline",
        destination="postgres",
        dataset_name="satair_master_data_dev",
        progress="log",
        dev_mode=True
    )

    # --- CONFIGURATION ---
    host = "ftsplus.xxx.corp"
    user = "xxx"
    raw_password = "x*x;x+x"
    
    # 1. Construct URL with USERNAME but NO PASSWORD
    # This tells fsspec "We are User X on Host Y", preventing anonymous login
    bucket_url = f"ftp://{user}@{host}:21/xxx-walter/master-data/dev/new"

    # 2. Pass ONLY the password in credentials
    # dlt will merge this with the connection info from the URL
    ftp_creds = {
        "password": raw_password
    }

    print(f"🚀 Configured URL: {bucket_url}")
    print("🚀 Injecting Raw Password via Credentials dictionary...")

    # 3. Define Source
    ftp_files = filesystem(
        bucket_url=bucket_url,
        file_glob="*.csv",
        credentials=ftp_creds
    )

    # 4. Run Pipeline
    reader = ftp_files | read_csv(chunk_size=1000)
    load_info = pipeline.run(reader.with_name("raw_csv_data"))
    print(load_info)

if __name__ == "__main__":
    load_satair_ftp_data()