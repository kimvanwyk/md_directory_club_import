from collections import defaultdict
import configparser
from enum import Enum
import operator

import attr
from sqlalchemy import create_engine, Table, MetaData, and_, or_, select

HANDLE_EXC = True
TABLE_PREFIX = "md_directory"


class DBHandler(object):
    def __init__(self, year, username, password, schema, host, port, db_type):
        self.year = year
        engine = create_engine(
            "%s://%s:%s@%s:%s/%s" % (db_type, username, password, host, port, schema),
            echo=False,
        )
        metadata = MetaData()
        metadata.bind = engine
        self.conn = engine.connect()
        tables = [
            t[0]
            for t in self.conn.execute("SHOW TABLES").fetchall()
            if TABLE_PREFIX in t[0]
        ]
        self.tables = {}
        for t in tables:
            self.tables[t.split(TABLE_PREFIX)[-1].strip("_")] = Table(
                t, metadata, autoload=True, schema=schema
            )

        self.club_id = None

        t = self.tables['officertitle']
        res = self.conn.execute(t.select()).fetchall()
        self.officers = {r.title: r.id for r in res}

    def set_club_id(self, club_id):
        self.club_id = club_id
        if not self.is_club_captured():
            self.club_id = None
            return False
        return True

    def is_club_captured(self):
        t = self.tables['club']
        res = self.conn.execute(t.select(t.c.id == self.club_id)).fetchone()
        return bool(res)

    def is_member_captured(self, member_id):
        t = self.tables['member']
        res = self.conn.execute(t.select(t.c.id == member_id)).fetchone()
        return bool(res)

    def insert_member(self, member_id, club_id, first_name, last_name, email, cell_ph):
        d = {'id': member_id,
             'club_id': club_id,
             'first_name': first_name,
             'last_name': last_name,
             'email': email,
             'cell_ph': cell_ph,
             'deceased_b': False,
             'resigned_b': False,
             'partner': '',
             'partner_lion_b': False,
             'home_ph': '',
             'bus_ph': '',
             'fax': '',
             'cell_ph': cell_ph or ''
        }
        t = self.tables['member']
        try:
            res = self.conn.execute(t.insert(d))
            return True
        except Exception as e:
            print(e)
            return False

    def update_officer(self, member_id, officer_title):
        if any((not self.is_member_captured(member_id),
                self.club_id is None,
                officer_title not in self.officers)):
            return False
        t = self.tables['clubofficer']
        d = {'year': self.year,
             'office_id': self.officers[officer_title],
             'member_id': member_id,
             'club_id': self.club_id,
             'po_code': '',
             'phone': '',
             'fax': '',
             'email': ''
        }
        for i in range(1,5):
            d[f'add{i}'] = ''
        try:
            res = self.conn.execute(t.select(and_(t.c.year == self.year-1, t.c.club_id == self.club_id))).fetchone()
            if res:
                d['email'] = res.email
            res = self.conn.execute(t.insert(d))
        except Exception as e:
            print(e)
            res = self.conn.execute(t.update(and_(t.c.club_id == self.club_id, t.c.year == self.year), d))
        return True
    
def get_db_settings(fn="db_settings.ini", sec="DB"):
    settings = {}
    cp = configparser.SafeConfigParser()
    with open(fn, "r") as fh:
        cp.readfp(fh)
    for opt in cp.options(sec):
        settings[opt] = cp.get(sec, opt)
    return settings


def get_db_handler(year, db_settings_fn="db_settings.ini", db_settings_sec="DB"):
    return DBHandler(year, **get_db_settings(db_settings_fn, db_settings_sec))
