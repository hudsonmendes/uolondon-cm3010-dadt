python3 -m pip install -U pip virtualenv
python3 -m virtualenv etl/venv
source etl/venv/bin/activate
python3 -m pip install -r etl/requirements.txt
python3 ./etl