#!/bin/bash
pip install -r requirements.txt --upgrade --target .
ls odevalidator
zip -r canary.zip main.py slacker.py odevalidator*/ sqs_client/
rm -rf odevalidator*/
echo "Created package in canary.zip"
