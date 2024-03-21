#!/bin/sh
export FLASK_APP=./receive-produce.py

pipenv run pip install -r  requirements.txt 
pipenv run flask run -h 0.0.0.0 -p 5001