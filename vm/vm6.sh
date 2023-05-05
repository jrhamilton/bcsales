#!/bin/bash

echo "Next provide your GCP PROJECT ID."
echo "If you do not have one please read the README instructions on how to make one."
echo "..."

echo -n "Provide your GCP PROJECT NAME ID:"
read -r gps_project

echo "export GCP_PROJECT=$gps_project" >> $HOME/.bashrc
echo "export GCS_BUCKET=bandcamp_sales_$gps_project" >> $HOME/.bashrc
export GCP_PROJECT=$gps_project
export GCS_BUCKET=bandcamp_sales_$gps_project

echo "..."
echo "GCP_PROJECT and GCS_BUCKET global variables are now set."
echo "GCP_PROJECT=$GCP_PROJECT"
echo "GCS_BUCKET=$GCS_BUCKET"
echo "..."

echo -n "Provide your Region:"
read -r region

echo "export GCP_REGION=$region" >> $HOME/.bashrc
export GCP_REGION=$region

echo "..."
echo "GCP_REGION=$GCP_REGION"



echo "locals {
  data_lake_bucket = \"bandcamp_sales\"
}

variable \"project\" {
  description = \"Your PROJECT_ID name\"
  default = \"$GCP_PROJECT\"
}

variable \"region\" {
  description = \"Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations\"
  type = string
  default = \"$GCP_REGION\"
}

variable \"storage_class\" {
  description = \"Storage class type for your bucket. Check official docs for more info.\"
  default = \"STANDARD\"
}

variable \"BQ_DATASET_STG\" {
  description = \"BigQuery Dataset that raw data (from GCS) will be written to\"
  type = string
  default = \"bandcamp_sales_schema\"
}

variable \"BQ_DATASET_PROD\" {
  description = \"BigQuery Dataset that raw data (from GCS) will be written to\"
  type = string
  default = \"bc_production\"
}

variable \"credentials\" {
  description = \"Credentials GOOGLE credentials found in ~/.creds/gcp/gac.json\"
  type = string
  default = \"/home/j/.creds/gcp/gac.json\"
}" > $HOME/bcsales/terraform/variable.tf

mkdir $HOME/.dbt

echo "bc_sales:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: \"{{ env_var('GCP_PROJECT') }}\"
      dataset: bandcamp_sales_schema
      threads: 4
      # These fields come from the service account json keyfile
      keyfile: \"{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}\"
    prod:
      type: bigquery
      method: service-account
      project: \"{{ env_var('GCP_PROJECT') }}\"
      dataset: bc_production
      threads: 4
      # These fields come from the service account json keyfile
      keyfile: \"{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}\"" > $HOME/.dbt/profiles.yml
