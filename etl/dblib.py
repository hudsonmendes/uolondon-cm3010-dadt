from abc import ABC
from dataclasses import dataclass
from typing import List, Set, Dict, Tuple
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
    property_types: "PropertyTypeRepository"
    properties: "PropertyRepository"

    def commit(self):
        self.conn.commit()


class BaseRepository(ABC):
    def __init__(self, conn: mysql.MySQLConnection) -> None:
        self.conn = conn


class PlaceRepository(BaseRepository):
    def ensure_ids_for(self, place_names: Set[str]) -> Dict[str, int]:
        place_names = [pn for pn in place_names if pn]
        if not place_names:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM places ORDER BY name")
                existing_names = set([row[0] for row in cursor])
                missing_names = sorted(place_names - existing_names)
                # inserting missing
                sql = "INSERT INTO places (name) VALUES (%s)"
                for missing_name in tqdm(missing_names, desc="inserting(places)"):
                    cursor.execute(sql, (missing_name,))
                # mapping
                cursor.execute("SELECT id, name FROM places ORDER BY name")
                return {place_name: place_id for (place_id, place_name) in cursor}


class PostgroupRepository(BaseRepository):
    def ensure_ids_for(self, postgroups: Set[str]) -> Dict[str, int]:
        postgroups = [pg for pg in postgroups if pg]
        if not postgroups:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM postgroups ORDER BY name")
                existing_names = set([row[0] for row in cursor])
                missing_names = sorted(postgroups - existing_names)
                # inserting missing
                sql = "INSERT INTO postgroups (name) VALUES (%s)"
                for missing_name in tqdm(missing_names, desc="inserting(postgroups)"):
                    cursor.execute(sql, (missing_name,))
                # mapping
                cursor.execute("SELECT id, name FROM postgroups ORDER BY name")
                return {postgroup: pg_id for (pg_id, postgroup) in cursor}


class PostcodeRepository(BaseRepository):
    def ensure_ids_for(self, postcodes: Set[str], postgroups_ids: Dict[str, int]) -> Dict[str, int]:
        postcodes = [pc for pc in postcodes if pc]
        if not postcodes:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM postcodes ORDER BY name")
                existing_names = set([row[0] for row in cursor])
                missing_names = sorted(postcodes - existing_names)
                # inserting missing
                sql = "INSERT INTO postcodes (postgroup_id, name) VALUES (%s, %s)"
                for missing_name in tqdm(missing_names, desc="inserting(postcodes)"):
                    missing_pg = missing_name.split(" ")[0]
                    missing_pg_id = postgroups_ids.get(missing_pg, None)
                    if missing_pg_id and missing_name:
                        cursor.execute(sql, (missing_pg_id, missing_name))
                # mapping
                cursor.execute("SELECT id, name FROM postcodes ORDER BY name")
                return {postcode: pc_id for (pc_id, postcode) in cursor}


class PropertyTypeRepository(BaseRepository):
    def ensure_ids_for(self, property_type_names: Set[str]) -> Dict[str, int]:
        property_type_names = [ptn for ptn in property_type_names if ptn]
        if not property_type_names:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM property_types ORDER BY name")
                existing_names = set([row[0] for row in cursor])
                missing_names = sorted(property_type_names - existing_names)
                # inserting missing
                sql = "INSERT INTO property_types (name) VALUES (%s)"
                for missing_name in tqdm(missing_names, desc="inserting(property_types)"):
                    cursor.execute(sql, (missing_name,))
                # mapping
                cursor.execute("SELECT id, name FROM property_types ORDER BY name")
                return {place_name: place_id for (place_id, place_name) in cursor}
        return {}


class PropertyRepository(BaseRepository):
    def ensure_ids_for(
        self,
        properties: Set[Tuple[str, str, str, str, str]],
        postcode_ids: Dict[str, int],
        property_type_ids: Dict[str, int],
    ) -> Dict[Tuple[str, str, str, str, str], int]:
        with self.conn.cursor() as cursor:
            # inserting missing
            sql = "REPLACE INTO postcodes (postgroup_id, name) VALUES (%s, %s)"
            for property in tqdm(properties, desc="inserting(properties)"):
                (postcode, property_type, number_or_name, building_ref, street_name) = property
                cursor.execute(
                    sql,
                    (
                        postcode_ids[postcode],
                        property_type_ids[property_type],
                        number_or_name,
                        building_ref,
                        street_name,
                    ),
                )
            # mapping
            cursor.execute(
                """
            SELECT id, pc.name `postcode`, pt.name `property_type`
                   number_or_name, building_ref, street_name
            FROM properties p
                INNER JOIN postcodes pc ON p.postcode_id = pc.id
                INNER JOIN property_types pt ON p.property_type_id = pt.id
            """
            )
            return {
                (postcode, property_type, number_or_name, building_ref, street_name): pid
                for (pid, postcode, property_type, number_or_name, building_ref, street_name) in cursor
            }


def repositories(config) -> Repositories:
    conn = mysql.connect(**config["mysql"])
    return Repositories(
        conn,
        places=PlaceRepository(conn),
        postgroups=PostgroupRepository(conn),
        postcodes=PostcodeRepository(conn),
        property_types=PropertyTypeRepository(conn),
        properties=PropertyRepository(conn),
    )
