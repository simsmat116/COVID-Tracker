#!/bin/bash

if [[ ! -d env ]]
then
  /usr/local/bin/python3 -m venv env
  source env/bin/activate
  sudo -H pip install -r requirements.txt
else
  source env/bin/activate
fi
