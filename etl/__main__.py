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

    pdd_count = 0
    pdd_record_loader = pdd_record_loader_factory(repositories)
    pdd_csvrows = stream_csv_from(folderpath=config["dataset"]["pdd_folder"])
    for pdd_csvrow in pdd_csvrows:
        pdd_record_loader(pddlib.transform(pdd_csvrow))
        if args.pdd_max_rows_each_file and args.pdd_max_rows_each_file > 0:
            pdd_count += 1
            if pdd_count >= args.pdd_max_rows_each_file:
                break

    ofsted_count = 0
    ofsted_record_loader = ofsted_record_loader_factory(repositories)
    ofsted_csvrows = stream_csv_from(folderpath=config["dataset"]["ofsted_folder"], encoding='cp1252', header=True)
    for ofsted_csvrow in ofsted_csvrows:
        ofsted_record_loader(ofstedlib.transform(ofsted_csvrow))
        if args.ofsted_max_rows_each_file and args.ofsted_max_rows_each_file > 0:
            ofsted_count += 1
            if ofsted_count >= args.ofsted_max_rows_each_file:
                break


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdd_max_rows_each_file", type=int, default=None, help="How many records to process")
    parser.add_argument("--ofsted_max_rows_each_file", type=int, default=None, help="How many records to process")
    return parser.parse_args()


def get_config():
    filename: str = "./config.ini"
    if not os.path.isfile(filename):
        shutil.copy("../config.ini", filename)
    config = configparser.ConfigParser()
    config.read("./config.ini")
    return config


def stream_csv_from(folderpath, encoding: str = 'utf-8', header: bool = False):
    total = 0
    for root, _, filenames in os.walk(folderpath):
        for filename in filenames:
            filepath = os.path.join(root, filename)
            with open(filepath, "r", encoding=encoding) as filehandle:
                total = sum(1 for _ in filehandle) - (1 if header else 0)
            with open(filepath, "r", encoding=encoding) as filehandle:
                if header:
                    next(filehandle, None)
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
            postgroup_id = r.postgroup.ensure_id(
                county_id, municipality_id, district_id, locality_id, record["postgroup"]
            )
            postcode_id = r.postcode.ensure_id(
                county_id, municipality_id, district_id, locality_id, postgroup_id, record["postcode"]
            )
            property_id = r.property.ensure_id(
                record["property_number_or_name"],
                record["building_or_block"],
                record["street_name"],
                postcode_id,
                postgroup_id,
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
    def load_record_to_db(record):
        if record:
            postcode = r.postcode.find_by_postcode(record["postcode"])
            if postcode:
                school_id = r.school.ensure_id(
                    record["name"],
                    postcode.postcode_id,
                    postcode.postgroup_id,
                    postcode.locality_id,
                    postcode.district_id,
                    postcode.municipality_id,
                    postcode.county_id,
                )
                education_phase_id = r.education_phase.ensure_id(record["phase_of_education"])
                r.rating.add(school_id, education_phase_id, record["overall_effectiveness"], record["ts"])
                r.commit()

    return load_record_to_db


if __name__ == "__main__":
    run()
