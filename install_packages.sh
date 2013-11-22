#!/bin/bash

sudo apt-get update
sudo apt-get -y install curl make g++ postgresql-9.1-postgis gdal-bin vim python-psycopg2 python-virtualenv libpq-dev python-dev git

sudo -u postgres createuser -D -R vagrant 
sudo -u postgres createdb -O vagrant djeocoder 

