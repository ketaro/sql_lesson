#!/usr/bin/env python
import csv
import sqlite3

DB          = None
CONN        = None
officers    = []
ships       = []
ranks       = {}
shipclasses = {}
shipids	    = {}
officerids  = {}

def connect_to_db():
	global DB, CONN

	CONN = sqlite3.connect("starfleet.db")
	DB   = CONN.cursor()


def create_tables():
	query = """
CREATE TABLE shipclass (
	id              INTEGER PRIMARY KEY AUTOINCREMENT,
	name            VARCHAR(20),
	crew_compliment INTEGER DEFAULT 0,
	cargo_bays      INTEGER DEFAULT 0,
	weapons         VARCHAR(50),
	shuttlecraft    INTEGER DEFAULT 0
)
"""
	DB.execute(query)

	query = """
CREATE TABLE ranks (
	id    INTEGER PRIMARY KEY AUTOINCREMENT,
	name  VARCHAR(30) NOT NULL
)
"""
	DB.execute(query)

	query = """
CREATE TABLE officers (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	givenname   VARCHAR(20) NOT NULL,
	surname     VARCHAR(20),
	rank		INTEGER,
	assigned_to INTEGER
)
"""
	DB.execute(query)

	query = """
CREATE TABLE ships (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	registry    VARCHAR(20) NOT NULL,
	name        VARCHAR(30),
	class_id    INTEGER,
	captain_id  INTEGER
)
"""
	DB.execute(query)


def read_files():
	global officers, ships

	# Read in the officers csv file, creating a dict for each officer and
	#   adding them to the officers list
	with open("starfleet-officers.csv", 'r') as csvfile:
		officer_reader = csv.reader(csvfile, delimiter=',')

		headers = None
		for row in officer_reader:
			officer = {}
			if not headers:
				headers = row
			else:
				for i in range(len(headers)):
					officer[headers[i]] = row[i]

			officers.append(officer)
	#		print "%r" % officer


	with open("starfleet-ships.csv", 'r') as csvfile:
		ship_reader = csv.reader(csvfile, delimiter=',')

		headers = None
		for row in ship_reader:
			ship = {}
			if not headers:
				headers = row
			else:
				for i in range(len(headers)):
					ship[headers[i]] = row[i]

			ships.append(ship)


def insert_rank(rank):
	query = """INSERT INTO ranks (name) VALUES (?)"""
	DB.execute(query, (rank,))
	CONN.commit()
	return DB.lastrowid

# Scan through each officer and build the rank dictionary
def make_ranks():
	global officers, ranks

	for officer in officers:
		rank = officer.get('Rank', '')
		if rank != '' and rank not in ranks:
			ranks[rank] = insert_rank(rank)
			print "Added Rank '%s' with id: %d" % (rank, ranks[rank])

# Create a ship class record using a ship as a template
def insert_shipclass(ship):
	query = """
INSERT INTO shipclass (name, crew_compliment, cargo_bays, weapons, shuttlecraft) 
VALUES (?, ?, ?, ?, ?)
"""
	DB.execute(query, (ship['Class'],
		               ship.get('Crew Compliment', 0),
		               ship.get('Cargo Bays', 0),
		               ship.get('Weapons', ''),
		               ship.get('Shuttlecraft', 0) ))
	CONN.commit()
	return DB.lastrowid

# Scan through each ship and build the ship classes
def make_classes():
	global ships, shipclasses

	for ship in ships:
		shipclass = ship.get('Class', '')
		if shipclass != '' and shipclass not in shipclasses:
			shipclasses[shipclass] = insert_shipclass(ship)
			print "Added Ship Class '%s' with id: %d" % (shipclass, shipclasses[shipclass])


# Scan through each ship and create them
def insert_ships():
	global ships, shipids, shipclasses

	for ship in ships:
		if ship.get('Name', '') != '':
			query = """INSERT INTO ships (registry, name, class_id) VALUES (?, ?, ?)"""
			DB.execute(query, (ship.get('Registration Number', 'unknown'), 
				               ship.get('Name', 'unknown'),
				               shipclasses.get(ship.get('Class', ''), 0) ))
			CONN.commit()
			shipids[ship.get('Registration Number', 'unknown')] = DB.lastrowid

			print "Added ship: '%s' with id: %d" % (ship.get('Name', 'unknown'), DB.lastrowid )

# Scan through each officer and create them
def insert_officers():
	global officers, officerids, ranks

	for officer in officers:
		query = """INSERT INTO officers (givenname, surname, rank, assigned_to) VALUES (?, ?, ?, ?)"""
		DB.execute(query, (officer.get('Given Name', ''), 
						   officer.get('Surname', ''),
						   ranks.get(officer.get('Rank', ''), 0),
						   shipids.get( officer.get('Assigned To ID', 0), 0)
			))
		CONN.commit()

		name = officer.get('Given Name', '')
		if 'Surname' in officer and officer['Surname'] != '':
			name = officer.get('Surname', '') + ', ' + name

		officerids[name] = DB.lastrowid

		print "Added Officer: %s with id: %d" % (name, DB.lastrowid)
		print "  assigned to: ", officer.get('Assigned To ID',  0)

# Assign Captains to each ship
def assign_captains():
	global officers, ships, officerids, shipids

	for ship in ships:
		shipid = shipids.get(ship.get('Registration Number', 'unknown'), 0)
		captainid = officerids.get(ship.get('Captain', ''), 0)

		query = """UPDATE ships SET captain_id = ? WHERE id = ?"""
		DB.execute(query, (captainid, shipid))

	CONN.commit()



# Do all the work
read_files()
connect_to_db()
create_tables()
make_ranks()
make_classes()
insert_ships()
insert_officers()
assign_captains()
