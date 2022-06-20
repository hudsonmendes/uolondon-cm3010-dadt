import configparser
import csv
import os
import shutil

import tqdm

import dblib
import pddlib
import ofstedlib


def run():
    config = get_config()
    repositories = dblib.repositories(config)
    etl_property_transactions(config, repositories)
    etl_ofsted_statistics(config, repositories)


def get_config():
    filename: str = "./config.ini"
    if not os.path.isfile(filename):
        shutil.copy("../config.ini", filename)
    config = configparser.ConfigParser()
    config.read("./config.ini")
    return config


def etl_property_transactions(config, repositories: dblib.Repositories):
    pdd_csvrows = stream_csv_from(folderpath=config["dataset"]["pdd_folder"])
    pdd_postgroups = set()
    pdd_postcodes = set()
    pdd_places = set()
    pdd_property_types = set()
    pdd_tenures = set()
    pdd_properties = set()
    pdd_pg_places = set()
    pdd_transactions = set()
    for _, pdd_csvrow in pdd_csvrows:
        # capture
        pdd_csvrow = pddlib.fix_critical_positions(pdd_csvrow)
        pdd_row_places = pddlib.get_places_from(pdd_csvrow)
        pdd_row_postcode = pddlib.get_postcode_from(pdd_csvrow)
        pdd_row_postgroup = pdd_row_postcode.split(" ")[0]
        pdd_row_property_type = pddlib.get_property_type_from(pdd_csvrow)
        pdd_row_tenure = pddlib.get_tenure_from(pdd_csvrow)
        pdd_row_property = pddlib.get_property_from(pdd_csvrow)
        pdd_row_transaction = pddlib.get_transaction_from(pdd_csvrow)
        # pile up
        pdd_postcodes.add(pdd_row_postcode)
        pdd_postgroups.add(pdd_row_postgroup)
        [pdd_places.add(pdd_place) for pdd_place in pdd_row_places if pdd_place]
        [pdd_pg_places.add((pdd_row_postgroup, pdd_place)) for pdd_place in pdd_row_places if pdd_place]
        pdd_property_types.add(pdd_row_property_type)
        pdd_tenures.add(pdd_row_tenure)
        pdd_properties.add(pdd_row_property)
        pdd_transactions.add(pdd_row_transaction)
    # store and get ids
    map_place_ids = repositories.places.ensure_ids_for(pdd_places)
    map_postgroups_ids = repositories.postgroups.ensure_ids_for(pdd_postgroups)
    map_postcodes_ids = repositories.postcodes.ensure_ids_for(pdd_postcodes, map_postgroups_ids)
    map_propert_type_ids = repositories.property_types.ensure_ids_for(pdd_property_types)
    map_property_ids = repositories.properties.ensure_ids_for(pdd_properties, map_postcodes_ids, map_propert_type_ids)
    map_tenure_ids = repositories.tenures.ensure_ids_for(pdd_tenures)
    repositories.places_postgroups.link(pdd_pg_places, map_postgroups_ids, map_place_ids)
    repositories.transactions.ensure(pdd_transactions, map_property_ids, map_tenure_ids)
    repositories.commit()


def etl_ofsted_statistics(config, repositories: dblib.Repositories):
    pass
    # ofsted_csvrows = stream_csv_from(folderpath=config["dataset"]["ofsted_folder"], encoding="cp1252", header=True)
    # ofsted_records = [ofstedlib.transform(h, r) for (h, r) in ofsted_csvrows]
    # assert ofsted_records


def stream_csv_from(folderpath, encoding: str = "utf-8", header: bool = False):
    csvheader = None
    total = 0
    for root, _, filenames in os.walk(folderpath):
        for filename in filenames:
            if filename.endswith(".csv"):
                filepath = os.path.join(root, filename)
                with open(filepath, "r", encoding=encoding) as filehandle:
                    total = sum(1 for _ in filehandle) - (1 if header else 0)
                with open(filepath, "r", encoding=encoding) as filehandle:
                    spamreader = csv.reader(filehandle)
                    if header:
                        csvheader = next(spamreader, None)
                        while not csvheader or not csvheader[0]:
                            csvheader = next(spamreader, None)
                    for csvrow in tqdm.tqdm(spamreader, desc=filename, total=total):
                        yield csvheader, csvrow


if __name__ == "__main__":
    run()
