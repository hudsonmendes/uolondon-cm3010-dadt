from abc import ABC
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

import mysql.connector as mysql


@dataclass(frozen=True)
class Repositories:
    conn: mysql.MySQLConnection
    tenure: "DomainTableRepository"
    county: "DomainTableRepository"
    district: "DomainTableRepository"
    municipality: "DomainTableRepository"
    locality: "DomainTableRepository"
    property: "DomainTableRepository"
    transaction: "TransactionRepository"

    def commit(self):
        self.conn.commit()


class BaseRepository(ABC):
    def __init__(self, conn: mysql.MySQLConnection) -> None:
        self.conn = conn


class DomainTableRepository(BaseRepository):
    def __init__(self, conn: mysql.MySQLConnection, table: str, fields: List[str]) -> None:
        super(DomainTableRepository, self).__init__(conn)
        self.table = table
        self.fields = fields

    def ensure_id(self, *args) -> int:
        tenure_id = self._find_id(args)
        if tenure_id is not None:
            return tenure_id
        with self.conn.cursor() as cursor:
            fields = ",".join(self.fields)
            slots = ",".join(["%s"] * len(args))
            cursor.execute(f"INSERT INTO {self.table} ({fields}) VALUES ({slots})", args)
        tenure_id = self._find_id(args)
        assert tenure_id is not None
        return tenure_id

    def _find_id(self, args) -> Optional[int]:
        with self.conn.cursor() as cursor:
            where = " AND ".join(f"{f}=%s" for f in self.fields)
            cursor.execute(f"SELECT id FROM {self.table} WHERE {where}", args)
            row = next(cursor, None)
            return row[0] if row else None


class TransactionRepository(BaseRepository):
    def add(self, property_id: int, tenure_id: int, price: float, new_build: bool, ts: datetime):
        with self.conn.cursor() as cursor:
            sql = """
            INSERT INTO property_transactions (property_id, tenure_id, price, new_build, ts)
            VALUES (%s, %s, %s, %s, %s)
            """
            params = (property_id, tenure_id, price, new_build, ts)
            cursor.execute(sql, params)


def repositories(config) -> Repositories:
    conn = mysql.connect(**config["mysql"])
    return Repositories(
        conn=conn,
        tenure=DomainTableRepository(conn=conn, table="tenures", fields=["name"]),
        county=DomainTableRepository(conn=conn, table="counties", fields=["name"]),
        municipality=DomainTableRepository(conn=conn, table="municipalities", fields=["county_id", "name"]),
        district=DomainTableRepository(conn=conn, table="districts", fields=["county_id", "municipality_id", "name"]),
        locality=DomainTableRepository(
            conn=conn, table="localities", fields=["county_id", "municipality_id", "district_id", "name"]
        ),
        property=DomainTableRepository(
            conn=conn,
            table="properties",
            fields=[
                "property_number_or_name",
                "building_or_block",
                "street_name",
                "postgroup",
                "postcode",
                "locality_id",
                "district_id",
                "municipality_id",
                "county_id",
            ],
        ),
        transaction=TransactionRepository(conn=conn),
    )