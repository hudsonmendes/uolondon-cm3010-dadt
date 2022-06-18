import argparse
import configparser
import csv
import os
import shutil

import tqdm

import dblib
import pddlib
import ofstedlib


def run():
    args = get_args()
    config = get_config()
    repositories = dblib.repositories(config)

    if not args.pdd_off:
        pdd_record_loader = pdd_record_loader_factory(repositories)
        pdd_csvrows = stream_csv_from(folderpath=config["dataset"]["pdd_folder"])
        for pdd_csvrow in pdd_csvrows:
            pdd_record_loader(pddlib.transform(pdd_csvrow))

    if not args.ofsted_off:
        ofsted_record_loader = ofsted_record_loader_factory(repositories)
        ofsted_csvrows = stream_csv_from(folderpath=config["dataset"]["ofsted_folder"])
        for ofsted_csvrow in ofsted_csvrows:
            ofsted_record_loader(ofstedlib.transform(ofsted_csvrow))


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdd_off", action="store_false", default=True, help="Prevent ETL for Price Paid Data")
    parser.add_argument("--ofsted_off", action="store_false", default=True, help="Prevent ETL for Ofsted Statistics")
    return parser.parse_args()


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


def pdd_record_loader_factory(r: dblib.Repositories):
    def load_record_to_db(record):
        if record:
            tenure_id = r.tenure.ensure_id(record["tenure_type"])
            county_id = r.county.ensure_id(record["county"])
            municipality_id = r.municipality.ensure_id(county_id, record["town_or_city"])
            district_id = r.district.ensure_id(county_id, municipality_id, record["district"])
            locality_id = r.locality.ensure_id(county_id, municipality_id, district_id, record["locality"])
            property_id = r.property.ensure_id(
                record["property_number_or_name"],
                record["building_or_block"],
                record["street_name"],
                record["postgroup"],
                record["postcode"],
                locality_id,
                district_id,
                municipality_id,
                county_id,
            )
            r.transaction.add(
                property_id=property_id,
                tenure_id=tenure_id,
                price=record["price"],
                new_build=record["new_build"],
                ts=record["ts"],
            )
            r.commit()

    return load_record_to_db


def ofsted_record_loader_factory(r: dblib.Repositories):
    return lambda x: None


if __name__ == "__main__":
    run()
