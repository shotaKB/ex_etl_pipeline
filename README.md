Description:<br>
Automated ETL pipeline for processing monthly payment reports. Reads raw csv/xlsx files, cleans them, and loads them to BQ

Project Structure:<br>
-creds/: Store google service acount json credential file here<br>
-exports/: bad_isrcs.csv gets exported here.<br>
-scripts/: Python scripts to run this program<br>
-source/: Drop raw data files here.<br>
-pipeline.yaml: Config for file names, GCS buckets, BQ datasets, etc.

Requirements:<br>
-Docker Desktop<br>
-A Google acount with access to Google Cloud Project<br>
-'creds/cred.json' Service Acount key with the following roles:<br>
    BigQuery Data Editor<br>
    BigQuery Job User<br>
    Storage Object Admin

Running the Pipeline<br>
1. Prepare Folders<br>
Add your input files to /source and your credential key to /creds

2. Build the Docker image<br>
docker build -t pipeline:local .

3. Run the container<br>
'''bash
docker run --rm \
  -v "$PWD/source:/app/source:ro" \
  -v "$PWD/exports:/app/exports:rw" \
  -v "$PWD/creds/cred.json:/creds/cred.json:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/creds/cred.json \
  -e GOOGLE_CLOUD_PROJECT=iron-tofu-111222 \
  pipeline:local

4. Check for errors<br>
Go to /exports and look at the csv files. These will list ISRCs that were not found in our music database