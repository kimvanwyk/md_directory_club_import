import csv
import sys

import db_handler

FH = "md410_club_data.csv"

OFFICERS = {'Club President': 'pres',
            'Club Secretary': 'sec',
            'Club Treasurer': 'treas',
            'Club Membership Chairperson': 'mem_chair',
            'Branch Coordinator': 'branch_coord',
            'Branch Liaison': 'branch_liason'
            }

db = db_handler.get_db_handler(2019)

with open(FH, 'r') as fh:
    dr = csv.DictReader(fh)
    for r in dr:
        club_id = int(r['Club ID'])
        member_id = int(r['Member ID'])
        if not db.set_club_id(club_id):
            print(f"Club {r['Club Name']} has not been captured")
        else:
            for (title, off) in OFFICERS.items():
                if title in r['Title']:
                    if not db.is_member_captured(r['Member ID']):
                        res = db.insert_member(member_id, club_id, r['First Name'], r['Last Name'], r['Email'], r['Cell Phone'])
                        if not res:
                            print(f"To insert: {r['First Name']} {r['Last Name']} ({member_id}) {r['Email']} {r['Cell Phone']}")
                    else:
                        res = db.update_officer(member_id, title)
                    # print(f"{r['Club Name']} ({r['Club ID']}): {off}: {r['Last Name']}. Club captured: {db.set_club_id(club_id)}. Member captured: {db.is_member_captured(r['Member ID'])}")
