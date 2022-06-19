import configparser
import csv
import itertools
import os
import shutil

import tqdm

import dblib
import pddlib
import ofstedlib


def run():
    config = get_config()
    repositories = dblib.repositories(config)

    pdd_csvrows = stream_csv_from(folderpath=config["dataset"]["pdd_folder"])
    pdd_places = sorted(set(itertools.chain(*[pddlib.get_places_from(pdd_csvrow) for (_, pdd_csvrow) in pdd_csvrows])))
    map_place_ids = repositories.places.ensure_ids_for(pdd_places)
    repositories.commit()
    assert map_place_ids

    pdd_csvrows = stream_csv_from(folderpath=config["dataset"]["pdd_folder"])
    pdd_postcodes = sorted(set(itertools.chain(*[pddlib.get_postcodes_from(pdd_csvrow) for (_, pdd_csvrow) in pdd_csvrows])))
    pdd_postgroups = sorted(set([ pc.split(' ') for pc in pdd_postcodes ]))
    map_postgroups_ids = repositories.postcodes.ensure_ids_for(pdd_postgroups)
    repositories.commit()
    assert map_postgroups_ids
    map_postcodes_ids = repositories.postcodes.ensure_ids_for(pdd_postcodes, map_postgroups_ids)
    repositories.commit()
    assert map_postcodes_ids

    ofsted_csvrows = stream_csv_from(folderpath=config["dataset"]["ofsted_folder"], encoding="cp1252", header=True)
    ofsted_records = [ofstedlib.transform(h, r) for (h, r) in ofsted_csvrows]
    assert ofsted_records


def get_config():
    filename: str = "./config.ini"
    if not os.path.isfile(filename):
        shutil.copy("../config.ini", filename)
    config = configparser.ConfigParser()
    config.read("./config.ini")
    return config


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
