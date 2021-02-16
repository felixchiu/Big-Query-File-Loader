# Big Query File Loader

## Summary
Loading csv file into Big query with attaching a batch number attached with the file. 

## Developer Setup

### Assumption
* Python 3.7 or above should be installed
* Python `virtualenv` should be installed

### Python and Virtualenv Setup
```bach
python3 -m venv venv/
source venv/bin/activate
pip install --upgrade pip
pip install google-cloud
pip install google-cloud-storage
pip install --upgrade google-cloud-bigquery
```