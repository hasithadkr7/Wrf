#!/usr/bin/env bash

# Print execution date time
echo `date`

# Change directory into where netcdf_data_uploader is located.
echo "Changing into ~/netcdf_data_uploader"
cd /home/uwcc-admin/netcdf_data_uploader
echo "Inside `pwd`"

# If no venv (python3 virtual environment) exists, then create one.
if [ ! -d "venv" ]
then
    echo "Creating venv python3 virtual environment."
    virtualenv -p python3 venv
fi

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate
#touch pusher.log
# Install dependencies using pip.
if [ ! -f "netcdf_reader.log" ]
then
    echo "Installing numpy"
    pip install numpy
    echo "Installing netCDF4"
    pip install netCDF4
    echo "Installing cftime"
    pip install cftime
    echo "Installing PyMySQL"
    pip install PyMySQL
    echo "Installing datalayer"
    pip install git+https://github.com/gihankarunarathne/CurwMySQLAdapter.git -U
fi

# Run email_notifier.py script.
echo "Running server_netcdf.py. Logs Available in netcdf_reader.log file."
#cmd >>file.txt 2>&1
#Bash executes the redirects from left to right as follows:
#  >>file.txt: Open file.txt in append mode and redirect stdout there.
#  2>&1: Redirect stderr to "where stdout is currently going". In this case, that is a file opened in append mode.
#In other words, the &1 reuses the file descriptor which stdout currently uses.
python server_netcdf.py >> netcdf_reader.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
