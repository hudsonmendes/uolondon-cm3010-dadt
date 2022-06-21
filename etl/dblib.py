from abc import ABC
from dataclasses import dataclass
from typing import Set, List, Dict, Any, Generator, Tuple
from datetime import datetime
from tqdm import tqdm

import mysql.connector as mysql

BATCH_SIZE = 250


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
    education_phases: "EducationPhaseRepository"
    schools: "SchoolRepository"
    ratings: "RatingRepository"

    def commit(self):
        self.conn.commit()


class BaseRepository(ABC):
    def __init__(self, conn: mysql.MySQLConnection) -> None:
        self.conn = conn

    def _batch_page(self, records: List[Any], desc: str) -> Generator[List[Any], None, None]:
        if records:
            batch = []
            for record in tqdm(records, desc=desc):
                if record:
                    batch.append(record)
                    if len(batch) >= BATCH_SIZE:
                        yield batch
                        batch.clear()
            if len(batch) > 0:
                yield batch


class PlaceRepository(BaseRepository):
    def ensure_ids_for(self, place_names: Set[str]) -> Dict[str, int]:
        place_names = set([pn for pn in place_names if pn])
        if not place_names:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM places")
                incoming_records = set((pn,) for pn in place_names)
                existing_records = set(cursor)
                missing_records = sorted(incoming_records - existing_records)
                # inserting missing
                for batch in self._batch_page(missing_records, "inserting(places)"):
                    sql = "INSERT INTO places (name) VALUES (%s)"
                    cursor.executemany(sql, batch)
                # mapping
                cursor.execute("SELECT id, name FROM places")
                return {place_name: place_id for (place_id, place_name) in cursor}


class PostgroupRepository(BaseRepository):
    def ensure_ids_for(self, postgroups: Set[str]) -> Dict[str, int]:
        postgroups = set([pg for pg in postgroups if pg])
        if not postgroups:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM postgroups")
                incoming_records = set((pg,) for pg in postgroups)
                existing_records = set(cursor)
                missing_records = sorted(incoming_records - existing_records)
                for batch in self._batch_page(missing_records, "inserting(postgroups)"):
                    sql = "INSERT INTO postgroups (name) VALUES (%s)"
                    cursor.executemany(sql, batch)
                # mapping
                cursor.execute("SELECT id, name FROM postgroups")
                return {postgroup: pg_id for (pg_id, postgroup) in cursor}


class PostcodeRepository(BaseRepository):
    def ensure_ids_for(self, postcodes: Set[str], pgids: Dict[str, int]) -> Dict[str, int]:
        postcodes = set(pc for pc in postcodes if pc)
        records = set((pgids.get(pc.split(" ")[0], None), pc) for pc in postcodes if pc)
        if not postcodes:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT postgroup_id, name FROM postcodes")
                incoming_records = set(r for r in records if r[0])
                existing_records = set(cursor)
                missing_records = sorted(incoming_records - existing_records)
                for batch in self._batch_page(missing_records, "inserting(postcodes)"):
                    sql = "INSERT INTO postcodes (postgroup_id, name) VALUES (%s, %s)"
                    cursor.executemany(sql, batch)
            return self.get_ids()

    def get_ids(self) -> Dict[str, int]:
        # mapping
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT id, name FROM postcodes")
            return {postcode: pc_id for (pc_id, postcode) in cursor}


class PlacePostgroupRepository(BaseRepository):
    def link(self, postgroup_places: Set[Tuple[str, str]], pgids: Dict[str, int], pids: Dict[str, int]) -> None:
        translated_links = set((pgids.get(pg, None), pids.get(p, None)) for (pg, p) in postgroup_places)
        with self.conn.cursor() as cursor:
            # collecting
            cursor.execute("SELECT postgroup_id, place_id FROM places_postgroups")
            inbound_links = set(l for l in translated_links if l[0] and l[1])
            exiting_links = set(cursor)
            missing_links = sorted(inbound_links - exiting_links)
            for batch in self._batch_page(missing_links, "linking(places|postgroups)"):
                sql = "INSERT INTO places_postgroups (postgroup_id, place_id) VALUES (%s, %s)"
                cursor.executemany(sql, batch)


class PropertyTypeRepository(BaseRepository):
    def ensure_ids_for(self, property_type_names: Set[str]) -> Dict[str, int]:
        property_type_names = set([ptn for ptn in property_type_names if ptn])
        if not property_type_names:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM property_types")
                inbound_records = set((pt,) for pt in property_type_names)
                existing_records = set(cursor)
                missing_records = sorted(inbound_records - existing_records)
                for batch in self._batch_page(missing_records, "inserting(property_types)"):
                    sql = "INSERT INTO property_types (name) VALUES (%s)"
                    cursor.executemany(sql, batch)
                # mapping
                cursor.execute("SELECT id, name FROM property_types")
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
                cursor.execute("SELECT name FROM tenures")
                inbound_records = set((pn,) for pn in place_names)
                existing_records = set(cursor)
                missing_records = sorted(inbound_records - existing_records)
                for batch in self._batch_page(missing_records, "inserting(tenures)"):
                    sql = "INSERT INTO tenures (name) VALUES (%s)"
                    cursor.executemany(sql, batch)
                # mapping
                cursor.execute("SELECT id, name FROM tenures")
                return {t_name: t_id for (t_id, t_name) in cursor}


class EducationPhaseRepository(BaseRepository):
    def ensure_ids_for(self, education_phases: Set[str]) -> Dict[str, int]:
        education_phases = set([ep for ep in education_phases if ep])
        if not education_phases:
            return {}
        else:
            with self.conn.cursor() as cursor:
                # collecting
                cursor.execute("SELECT name FROM education_phases")
                inbound_records = set((ep,) for ep in education_phases)
                existing_records = set(cursor)
                missing_records = sorted(inbound_records - existing_records)
                for batch in self._batch_page(missing_records, "inserting(ep)"):
                    sql = "INSERT INTO education_phases (name) VALUES (%s)"
                    cursor.executemany(sql, batch)
                # mapping
                cursor.execute("SELECT id, name FROM education_phases")
                return {ep_name: ep_id for (ep_id, ep_name) in cursor}


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
            inbound_records = [r for r in records if r[0] and r[1]]
            for batch in self._batch_page(inbound_records, "inserting(properties)"):
                cursor.executemany(sql, batch)
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
            inbound_records = [r for r in records if r[0] and r[1]]
            for batch in self._batch_page(inbound_records, "inserting(transactions)"):
                cursor.executemany(sql, batch)


class SchoolRepository(BaseRepository):
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
            INSERT IGNORE INTO schools
            (postcode_id, name)
            VALUES
            (%s, %s)
            """
            inbound_records = [r for r in records if r[0] and r[1]]
            for batch in self._batch_page(inbound_records, "inserting(schools)"):
                cursor.executemany(sql, batch)
            # mapping
            cursor.execute(
                """
                SELECT s.id, pc.name `postcode`, s.name
                FROM schools s INNER JOIN postcodes pc ON s.postcode_id = pc.id
                """
            )
            return {(postcode, name): sid for (sid, postcode, name) in cursor}


class RatingRepository(BaseRepository):
    def ensure(
        self,
        ratings: Set[str, str, str, float],
        sids: Dict[Tuple[str, str], int],
        epids: Dict[str, int],
    ):
        records = set(
            (sids.get((pc, name), None), epids.get(ep, None), rating, ts) for (pc, name, ep, rating, ts) in ratings
        )
        with self.conn.cursor() as cursor:
            # inserting missing
            sql = """
            INSERT IGNORE INTO school_ratings
            (school_id, education_phase_id, rating, ts)
            VALUES
            (%s, %s, %s, %s)
            """
            inbound_records = [r for r in records if r[0] and r[1]]
            for batch in self._batch_page(inbound_records, "inserting(ratings)"):
                cursor.executemany(sql, batch)


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
        education_phases=EducationPhaseRepository(conn),
        schools=SchoolRepository(conn),
        ratings=RatingRepository(conn),
    )
