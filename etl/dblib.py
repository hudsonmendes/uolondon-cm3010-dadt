from abc import ABC
from dataclasses import dataclass
from typing import Set, Dict, Tuple
from datetime import datetime
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
    places_postgroups: "PlacePostgroupRepository"
    property_types: "PropertyTypeRepository"
    tenures: "TenureRepository"
    properties: "PropertyRepository"
    transactions: "TransactionRepository"

    def commit(self):
        self.conn.commit()


class BaseRepository(ABC):
    def __init__(self, conn: mysql.MySQLConnection) -> None:
        self.conn = conn


class PlaceRepository(BaseRepository):
    def ensure_ids_for(self, place_names: Set[str]) -> Dict[str, int]:
        place_names = set([pn for pn in place_names if pn])
        if not place_names:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM places ORDER BY name")
                incoming_records = set((pn,) for pn in place_names)
                existing_records = set(cursor)
                missing_records = sorted(incoming_records - existing_records)
                missing_iterator = tqdm(missing_records, desc="inserting(places)")
                # inserting missing
                sql = "INSERT INTO places (name) VALUES (%s)"
                cursor.executemany(sql, missing_iterator)
                # mapping
                cursor.execute("SELECT id, name FROM places ORDER BY name")
                return {place_name: place_id for (place_id, place_name) in cursor}


class PostgroupRepository(BaseRepository):
    def ensure_ids_for(self, postgroups: Set[str]) -> Dict[str, int]:
        postgroups = set([pg for pg in postgroups if pg])
        if not postgroups:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM postgroups ORDER BY name")
                incoming_records = set((pg,) for pg in postgroups)
                existing_records = set(cursor)
                missing_records = sorted(incoming_records - existing_records)
                missing_iterator = tqdm(missing_records, desc="inserting(postgroups)")
                # inserting missing
                sql = "INSERT INTO postgroups (name) VALUES (%s)"
                cursor.executemany(sql, missing_iterator)
                # mapping
                cursor.execute("SELECT id, name FROM postgroups ORDER BY name")
                return {postgroup: pg_id for (pg_id, postgroup) in cursor}


class PostcodeRepository(BaseRepository):
    def ensure_ids_for(self, postcodes: Set[str], pgids: Dict[str, int]) -> Dict[str, int]:
        postcodes = set([pc for pc in postcodes if pc])
        if not postcodes:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT postgroup_id, name FROM postcodes")
                incoming_records = set(
                    (
                        pgids.get(pc.strip(" ")[0], None),
                        pc,
                    )
                    for pc in postcodes
                )
                existing_records = set(cursor)
                missing_records = sorted(incoming_records - existing_records)
                missing_iterator = tqdm(missing_records, desc="inserting(postcodes)")

                # inserting missing
                sql = "INSERT INTO postcodes (postgroup_id, name) VALUES (%s, %s)"
                cursor.executemany(sql, missing_iterator)
                # mapping
                cursor.execute("SELECT id, name FROM postcodes ORDER BY name")
                return {postcode: pc_id for (pc_id, postcode) in cursor}


class PlacePostgroupRepository(BaseRepository):
    def link(self, postgroup_places: Set[Tuple[str, str]], pgids: Dict[str, int], pids: Dict[str, int]) -> None:
        translated_links = set([(pgids[pg], pids[p]) for (pg, p) in postgroup_places])
        with self.conn.cursor() as cursor:
            # collecting
            cursor.execute("SELECT postgroup_id, place_id FROM places_postgroups")
            exiting_links = set(row for row in cursor)
            missing_links = translated_links - exiting_links
            missing_iterator = tqdm(missing_links, desc="linking(postgroup/place)")

            # inserting missing
            sql = "INSERT INTO places_postgroups (postgroup_id, place_id) VALUES (%s, %s)"
            cursor.executemany(sql, missing_iterator)


class PropertyTypeRepository(BaseRepository):
    def ensure_ids_for(self, property_type_names: Set[str]) -> Dict[str, int]:
        property_type_names = set([ptn for ptn in property_type_names if ptn])
        if not property_type_names:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM property_types ORDER BY name")
                inbound_records = set((pt,) for pt in property_type_names)
                existing_records = set(cursor)
                missing_records = sorted(inbound_records - existing_records)
                missing_iterator = tqdm(missing_records, desc="inserting(property_types)")
                # inserting missing
                sql = "INSERT INTO property_types (name) VALUES (%s)"
                cursor.executemany(sql, missing_iterator)
                # mapping
                cursor.execute("SELECT id, name FROM property_types ORDER BY name")
                return {pt_name: pt_id for (pt_id, pt_name) in cursor}
        return {}


class TenureRepository(BaseRepository):
    def ensure_ids_for(self, place_names: Set[str]) -> Dict[str, int]:
        place_names = set([pn for pn in place_names if pn])
        if not place_names:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM tenures ORDER BY name")
                inbound_records = set((pn,) for pn in place_names)
                existing_records = set(cursor)
                missing_records = sorted(inbound_records - existing_records)
                missing_iterator = tqdm(missing_records, desc="inserting(tenures)")
                # inserting missing
                sql = "INSERT INTO tenures (name) VALUES (%s)"
                cursor.executemany(sql, missing_iterator)
                # mapping
                cursor.execute("SELECT id, name FROM tenures ORDER BY name")
                return {t_name: t_id for (t_id, t_name) in cursor}


class PropertyRepository(BaseRepository):
    def ensure_ids_for(
        self,
        properties: Set[Tuple[str, str, str, str, str]],
        pcids: Dict[str, int],
        ptids: Dict[str, int],
    ) -> Dict[Tuple[str, str, str, str, str], int]:
        records = set((pcids.get(pc, None), ptids.get(pt, None), non, br, sn) for (pc, pt, non, br, sn) in properties)
        with self.conn.cursor() as cursor:
            # inserting missing
            sql = """
            INSERT IGNORE INTO properties
            (postcode_id, property_type_id, number_or_name, building_ref, street_name)
            VALUES
            (%s, %s, %s, %s, %s)
            """
            inbound_records = set(r for r in records if r[0] and r[1])
            inbound_iterator = tqdm(inbound_records, desc="inserting(properties)")
            cursor.executemany(sql, inbound_iterator)
            # mapping
            cursor.execute(
                """
            SELECT p.id, pc.name `postcode`, pt.name `property_type`,
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


class TransactionRepository(BaseRepository):
    def ensure(
        self,
        transactions: Set[Tuple[Tuple[str, str, str, str, str], str, bool, float, datetime]],
        pids: Dict[Tuple[str, str, str, str, str], int],
        tids: Dict[str, int],
    ):
        records = set(
            (pids.get(p, None), tids.get(t, None), new_build, price, ts)
            for (p, t, new_build, price, ts) in transactions
            if price and ts
        )
        with self.conn.cursor() as cursor:
            # inserting missing
            sql = """
            INSERT IGNORE INTO property_transactions
            (property_id, tenure_id, new_build, price, ts)
            VALUES
            (%s, %s, %s, %s, %s)
            """
            inbound_records = set(r for r in records if r[0] and r[1])
            inbound_iterator = tqdm(inbound_records, desc="inserting(transactions)")
            cursor.executemany(sql, inbound_iterator)


def repositories(config) -> Repositories:
    conn = mysql.connect(**config["mysql"])
    return Repositories(
        conn,
        places=PlaceRepository(conn),
        postgroups=PostgroupRepository(conn),
        postcodes=PostcodeRepository(conn),
        places_postgroups=PlacePostgroupRepository(conn),
        property_types=PropertyTypeRepository(conn),
        tenures=TenureRepository(conn),
        properties=PropertyRepository(conn),
        transactions=TransactionRepository(conn),
    )
