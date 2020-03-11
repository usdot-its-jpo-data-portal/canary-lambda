#!/bin/bash

# NOTE!
# This script is deprecated and was intended for manual deployment packaging,
# Refer to the Dockerfile of this repository for the updated deployment process.

pip install -r requirements.txt --upgrade --target .
ls odevalidator
zip -r canary.zip main.py slacker.py odevalidator*/ sqs_client/
rm -rf odevalidator*/
echo "Created package in canary.zip"
