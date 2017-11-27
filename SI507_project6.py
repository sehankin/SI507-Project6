# Import statements

import psycopg2
import psycopg2.extras
from config import *
import csv


# Write code / functions to set up database connection and cursor here.

def get_connection_and_cursor():
    try:
        if db_password != "":
            db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            print("Success connecting to database")
        else:
            db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
    except:
        print("Unable to connect to the database. Check server and credentials.")
        sys.exit(1) # Stop running program if there's no db connection.
    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return db_connection, db_cursor

db_connection, db_cursor = get_connection_and_cursor()


# Write code / functions to create tables with the columns you want and all database setup here.

def setup_database(print_alert = False):
    db_cursor.execute("DROP TABLE IF EXISTS States CASCADE")  # better to do CREATE TABLE IF NOT EXISTS States, a la line 130 in section10_itunes_database.py
    db_cursor.execute("DROP TABLE IF EXISTS Sites CASCADE")
    db_cursor.execute("CREATE TABLE States(ID SERIAL PRIMARY KEY, Name VARCHAR(40) UNIQUE)")
    db_cursor.execute("CREATE TABLE Sites(ID SERIAL PRIMARY KEY, Name VARCHAR(128) UNIQUE, Type VARCHAR(128), State_ID INTEGER REFERENCES States(ID), Location VARCHAR(255), Description TEXT)")
    db_connection.commit()
    if print_alert is True:
        print("TABLES CREATED\n")


# Write code / functions to deal with CSV files and insert data into the database here.

def read_csv_file(csv_file_to_read, print_alert = False):
    state_csv = open(csv_file_to_read, "r")
    state_csv_reader = csv.reader(state_csv)
    sites_list = []
    for row in state_csv_reader:
        sites_list.append(row)
    state_csv.close()
    if print_alert is True:
        print("CSV FILE READ\n")
    return sites_list

def create_csv_state_string(state_csv_file_to_string, print_alert = False):
    initial_csv_state_string = state_csv_file_to_string[:-4]
    capitalize_first_letter = initial_csv_state_string[0].upper()
    final_csv_state_string = capitalize_first_letter + initial_csv_state_string[1:]
    if print_alert is True:
        print("STATE STRING CREATED\n")
    return final_csv_state_string

def create_list_of_site_dcts(state_csv_file_to_read, print_alert = False):
    csv_sites_list = read_csv_file(state_csv_file_to_read)
    list_of_site_dcts = []
    for site in csv_sites_list:
        site_dct = {}
        site_name_stripped = site[0].strip()
        site_dct["name"] = site_name_stripped
        site_location_stripped = site[1].strip()
        site_dct["location"] = site_location_stripped
        site_type_stripped = site[2].strip()
        site_dct["type"] = site_type_stripped
        site_address_stripped = site[3].strip()
        site_dct["address"] = site_address_stripped
        site_description_stripped = site[4].strip()
        site_dct["description"] = site_description_stripped
        list_of_site_dcts.append(site_dct)
    if print_alert is True:
        print("LIST OF SITE DCTS CREATED\n")
    return list_of_site_dcts[1:]

def insert_state_into_states_table(state_csv_file_to_insert, print_alert = False):
    state_string = create_csv_state_string(state_csv_file_to_insert)
    db_cursor.execute("INSERT INTO States(Name) VALUES(%s) ON CONFLICT DO NOTHING RETURNING ID", (state_string,))  # because we set States(Name) as UNIQUE, ON CONFLICT DO NOTHING serves to prevent the insertion if the state_string is already in the table
    state_id = db_cursor.fetchone()
    db_connection.commit()
    if print_alert is True:
        print("STATE INSERTED INTO STATES TABLE\n")
    return state_id

def insert_site_data_into_sites_table(state_csv_file_to_insert, print_alert = False):
    state_id_foreign_key = insert_state_into_states_table(state_csv_file_to_insert)["id"]
    list_of_site_dcts = create_list_of_site_dcts(state_csv_file_to_insert)
    for site_dct in list_of_site_dcts:
        db_cursor.execute("INSERT INTO Sites(Name, Type, State_ID, Location, Description) VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (site_dct["name"], site_dct["type"], state_id_foreign_key, site_dct["location"], site_dct["description"]))
    db_connection.commit()
    if print_alert is True:
        print("SITES INSERTED INTO SITES TABLE\n")


# Write code to be invoked here (e.g. invoking any functions you wrote above)

setup_database()
insert_site_data_into_sites_table("arkansas.csv")
insert_site_data_into_sites_table("california.csv")
insert_site_data_into_sites_table("michigan.csv")


# Write code to make queries and save data in variables here.

print_query_results = False

def query_and_return(query):
    db_cursor.execute(query)
    results = db_cursor.fetchall()
    return results  # a list of dictionaries

all_locations_results = query_and_return("SELECT Location FROM Sites")
all_locations = []
for result in all_locations_results:
    all_locations.append(result["location"])
if print_query_results is True:
    print(all_locations)
    print("\n")

beautiful_sites_results = query_and_return("SELECT Name FROM Sites WHERE Description LIKE '%beautiful%' OR Description LIKE '%Beautiful%'")
beautiful_sites = []
for result in beautiful_sites_results:
    beautiful_sites.append(result["name"])
if print_query_results is True:
    print(beautiful_sites)
    print("\n")

natl_lakeshores_results = query_and_return("SELECT COUNT(*) FROM Sites WHERE Type LIKE '%National Lakeshore%'")
natl_lakeshores = natl_lakeshores_results[0]["count"]
if print_query_results is True:
    print(natl_lakeshores)
    print("\n")

michigan_names_results = query_and_return("SELECT Sites.Name FROM Sites INNER JOIN States ON (Sites.State_ID = States.ID) WHERE States.Name LIKE '%Michigan%'")
michigan_names = []
for result in michigan_names_results:
    michigan_names.append(result["name"])
if print_query_results is True:
    print(michigan_names)
    print("\n")

total_number_arkansas_results = query_and_return("SELECT COUNT(*) FROM Sites INNER JOIN States ON (Sites.State_ID = States.ID) WHERE States.Name LIKE '%Arkansas%'")
total_number_arkansas = total_number_arkansas_results[0]["count"]
if print_query_results is True:
    print(total_number_arkansas)
    print("\n")
