#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.google/credentials/gcp-cred.json

echo "Next Executing: gcloud auth application-default login --no-browser"
echo "Then follow instructions. Will need to go back to local computer to finish authentication."
echo "****"
echo "When complete, continue with next script (vm4.sh)"

gcloud auth application-default login --no-browser

export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.creds/gcp/gac.json
echo "export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.creds/gcp/gac.json" >> $HOME/.bashrc
