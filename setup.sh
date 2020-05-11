#!/bin/bash

if [[ ! -d env ]]
then
  # Create virtual env and install dependencies
  /usr/local/bin/python3 -m venv env
  source env/bin/activate
  sudo -H pip install -r requirements.txt
else
  source env/bin/activate
fi

# Set the airflow home variable
export AIRFLOW_HOME="$(pwd)/env/lib/python3.7/site-packages/airflow"
echo $AIRFLOW_HOME
