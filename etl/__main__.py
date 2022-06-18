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
    repositories = dblib.repositories(config)
    load_record_to_db = record_loader(repositories)
    for csvrow in csvreader:
        load_record_to_db(transform_line_to_pdd(csvrow))


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


def record_loader(r: dblib.Repositories):
    def load_record_to_db(record):
        if record:
            tenure_id = r.tenure.ensure_id(record["tenure_type"])
            county_id = r.county.ensure_id(record["county"])
            municipality_id = r.municipality.ensure_id(county_id, record["town_or_city"])
            district_id = r.district.ensure_id(county_id, municipality_id, record["district"])
            locality_id = r.locality.ensure_id(county_id, municipality_id, district_id, record["locality"])
            r.commit()

    return load_record_to_db


if __name__ == "__main__":
    run()
