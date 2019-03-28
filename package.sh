#!/bin/bash
mkdir package
pip install git+https://github.com/usdot-jpo-ode/ode-output-validator-library.git --upgrade --target ./package
zip -r odevalidator.zip main.py package/
echo "Lambda package created in function.zip"
