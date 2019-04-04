#!/bin/bash
pip install -r requirements.txt --upgrade --target .
zip -r canary.zip main.py odevalidator.py result.py sequential.py bsmLogDuringEvent.ini odevalidator*/
echo "Created package in canary.zip"
