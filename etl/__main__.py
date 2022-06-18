import configparser
import csv
import os
import shutil

import tqdm

import dblib
import pddlib


def run():
    config = get_config()
    csvreader = stream_csv_from(folderpath=config["dataset"]["folder"])
    for csvrow in csvreader:
        record = transform_line_to_pdd(csvrow)
        if record:
            load_record_to_db(record)


def get_config():
    filename: str = "./config.ini"
    if not os.path.isfile(filename):
        shutil.copy("../config.ini", filename)
    config = configparser.ConfigParser()
    config.read("./config.ini")
    return config


def stream_csv_from(folderpath):
    total = 0
    for root, _, filenames in os.walk(folderpath):
        for filename in filenames:
            filepath = os.path.join(root, filename)
            with open(filepath, "r", encoding="utf-8") as filehandle:
                total = sum(1 for _ in filehandle)
            with open(filepath, "r", encoding="utf-8") as filehandle:
                spamreader = csv.reader(filehandle)
                yield from tqdm.tqdm(spamreader, desc=filename, total=total)


def transform_line_to_pdd(csvrow):
    return pddlib.transform(csvrow)


def load_record_to_db(record):
    tenure_id = dblib.ensure_tenure(name=record["tenure_type"])
    print(f"Tenure: #{tenure_id}")


if __name__ == "__main__":
    run()
