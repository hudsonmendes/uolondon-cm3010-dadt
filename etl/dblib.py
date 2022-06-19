from abc import ABC
from dataclasses import dataclass
from typing import List, Dict
from tqdm import tqdm

import mysql.connector as mysql


@dataclass(frozen=True)
class Postcode:
    county_id: int
    municipality_id: int
    district_id: int
    locality_id: int
    postgroup_id: int
    postcode_id: int
    name: str


@dataclass(frozen=True)
class Repositories:
    conn: mysql.MySQLConnection
    places: "PlaceRepository"
    postgroups: "PostgroupRepository"
    postcodes: "PostcodeRepository"

    def commit(self):
        self.conn.commit()


class BaseRepository(ABC):
    def __init__(self, conn: mysql.MySQLConnection) -> None:
        self.conn = conn


class PlaceRepository(BaseRepository):
    def ensure_ids_for(self, place_names: List[str]) -> Dict[str, int]:
        place_names = [pn for pn in place_names if pn]
        if place_names:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM places ORDER BY name")
                existing_names = set([row[0] for row in cursor])
                missing_names = sorted(set(place_names) - existing_names)
                # inserting missing
                sql = "INSERT INTO places (name) VALUES (%s)"
                for missing_name in tqdm(missing_names, desc="inserting(places)"):
                    cursor.execute(sql, (missing_name,))
                # mapping
                cursor.execute("SELECT id, name FROM places ORDER BY name")
                return {place_name: place_id for (place_id, place_name) in cursor}


class PostgroupRepository(BaseRepository):
    def ensure_ids_for(self, postgroups: List[str]) -> Dict[str, int]:
        postgroups = [pg for pg in postgroups if pg]
        if postgroups:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM postgroups ORDER BY name")
                existing_names = set([row[0] for row in cursor])
                missing_names = sorted(set(postgroups) - existing_names)
                # inserting missing
                sql = "INSERT INTO postgroups (name) VALUES (%s)"
                for missing_name in tqdm(missing_names, desc="inserting(postgroups)"):
                    cursor.execute(sql, (missing_name,))
                # mapping
                cursor.execute("SELECT id, name FROM postgroups ORDER BY name")
                return {postgroup: pg_id for (pg_id, postgroup) in cursor}


class PostcodeRepository(BaseRepository):
    def ensure_ids_for(self, postcodes: List[str], postgroups_map: Dict[str, int]) -> Dict[str, int]:
        postcodes = [pc for pc in postcodes if pc]
        if postcodes:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM postcodes ORDER BY name")
                existing_names = set([row[0] for row in cursor])
                missing_names = sorted(set(postcodes) - existing_names)
                # inserting missing
                sql = "INSERT INTO postcodes (postgroup_id, name) VALUES (%s, %s)"
                for missing_name in tqdm(missing_names, desc="inserting(postcodes)"):
                    cursor.execute(sql, (postgroups_map[missing_name], missing_name))
                # mapping
                cursor.execute("SELECT id, name FROM postcodes ORDER BY name")
                return {postcode: pc_id for (pc_id, postcode) in cursor}


def repositories(config) -> Repositories:
    conn = mysql.connect(**config["mysql"])
    return Repositories(
        conn, places=PlaceRepository(conn), postgroups=PostgroupRepository(conn), postcodes=PostcodeRepository(conn)
    )
