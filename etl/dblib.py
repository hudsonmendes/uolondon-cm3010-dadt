from abc import ABC
from dataclasses import dataclass
from typing import List, Optional

import mysql.connector as mysql


@dataclass(frozen=True)
class Repositories:
    conn: mysql.MySQLConnection
    tenure: "DomainTableRepository"
    county: "DomainTableRepository"
    district: "DomainTableRepository"
    municipality: "DomainTableRepository"
    locality: "DomainTableRepository"

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
    )
