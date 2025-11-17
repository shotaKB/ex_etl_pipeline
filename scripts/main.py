import io
import pathlib
import yaml
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from google.cloud import storage, bigquery
from scripts import report_cleanup

def set_report_dates(source):
    # First day of two months ago
    dt = date.today().replace(day=1) - relativedelta(months=2)
    report_date = dt.strftime("%Y-%m-%d")
    month_string = dt.strftime('%Y%m')
    
    return report_date, month_string

def gcs_upload(project, source, object_name, df):
    gcs = storage.Client(project=project)
    bucket = gcs.bucket(source["bucket"])
    buf = io.BytesIO(); df.to_csv(buf, index=False); buf.seek(0)
    bucket.blob(object_name).upload_from_file(buf, content_type="text/csv", rewind=True)

def bq_append(project, source, object_name):
    bq = bigquery.Client(project=project)
    dataset = source["dataset"]
    table_id = f"{project}.{dataset}.{source['table']}"
    job_cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition="WRITE_APPEND",
    )
    gcs_uri = f"gs://{source['bucket']}/{object_name}"
    bq.load_table_from_uri(gcs_uri, table_id, job_config=job_cfg).result()

def archive_files(project, source, object_name):
    gcs = storage.Client(project=project)
    bucket = gcs.bucket(source["bucket"])

    #Current location
    src_blob = bucket.blob(object_name)
    #New location in the same bucket
    archive_name = f"processed/{object_name}"

    #Copy then delete
    bucket.copy_blob(src_blob, bucket, archive_name)
    src_blob.delete()

def export_bad_isrcs(bad_isrcs, source, month_string):
    if bad_isrcs is None or bad_isrcs.empty:
        return
    
    export_dir = pathlib.Path("/app/exports")
    bad_isrcs_file = export_dir / f"bad_isrcs_{source['name']}_{month_string}.csv"
    bad_isrcs.to_csv(bad_isrcs_file, index=False)
        
def run():
    # Load config
    cfg = yaml.safe_load(open("pipeline.yaml"))
    project = cfg["project_id"]

    # Loop through each source in yaml
    for source in cfg["sources"]:
        print(f"Processing source: {source['name']}")

        # Set the date strings
        report_date, month_string = set_report_dates(source)

        # Data clensing
        df_clean, bad_isrcs = report_cleanup.run(source, report_date)

        if df_clean is None:
            print(f"No files found for pattern: {source['pattern']}")
            continue

        # Upload to GCS
        object_name = f"{source['name']}_{month_string}.csv"
        gcs_upload(project, source, object_name, df_clean)

        # load to BigQuery
        bq_append(project, source, object_name)

        # Move the processed files to archive
        archive_files(project, source, object_name)
        
        # Export unmatched songs as csv
        export_bad_isrcs(bad_isrcs, source, month_string)

if __name__ == "__main__":
    run()
