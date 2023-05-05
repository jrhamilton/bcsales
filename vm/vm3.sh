#!/bin/bash

echo "export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.google/credentials/gcp-cred.json" >> $HOME/.bashrc
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.google/credentials/gcp-cred.json

echo "RUN: source ~/.bashrc"
echo "Next Execute: gcloud auth application-default login --no-browser"
echo "Then follow instructions. Will need to go back to local computer to finish authentication."
echo "****"
echo "When complete, continue with next script (vm4.sh)"

gcloud auth application-default login --no-browser
