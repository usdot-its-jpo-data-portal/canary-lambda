#!/bin/bash
pip install -r requirements.txt --upgrade --target .
zip -r canary.zip main.py slacker.py odevalidator*/
rm -rf odevalidator*/
echo "Created package in canary.zip"
