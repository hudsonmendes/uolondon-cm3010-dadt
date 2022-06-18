# Find Regions with Good Schools & Good Home Prices


## Data ETL

**Important:** the following setup steps have already been done in this environment.
If for any reason the database does not have the data, please re-run the following steps
to reset the database installation.

```
python3 -m pip install -U pip virtualenv
python3 -m virtualenv etl/venv
source etl/venv/bin/activate
python3 -m pip install -r etl/requirements.txt
python3 -m ./etl
```