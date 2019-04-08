#!/bin/bash
pip install -r requirements.txt --upgrade --target .
zip -r canary.zip main.py emailer.py bsmLogDuringEvent.ini odevalidator*/
rm -rf odevalidator*/
echo "Created package in canary.zip"
