from mysql.connector.errors import Error
import sys
import mysql.connector
import os

import config
from config import config_db
from config import config_warehouse
import psycopg2

try:
    db = mysql.connector.connect(**config_db)
except Error as e:
    print(e)
    exit(1)

try:
    warehouse = psycopg2.connect(**config_warehouse)
except Error as e:
    print(e)
    exit(1)

sql = db.cursor()
postgres = warehouse.cursor()

#This query restrives all the data from the database
queryJoin = ("""SELECT *
FROM report_content join discharges d on report_content.id = d.rcid
join expenses e on report_content.id = e.rcid
join inpatient_revenue ir on report_content.id = ir.rcid
join outpatient_revenue o on report_content.id = o.rcid
join patient_days pd on report_content.id = pd.rcid
join utilization u on report_content.id = u.rcid
join visits v on report_content.id = v.rcid
join report r on report_content.rid = r.id
join hospital h on report_content.hid = h.id""")
sql.execute(queryJoin)
join = sql.fetchall()

# sql.execute("SELECT id, name, county, planning_area, control_type, type, phone, address, city, zipcode, ceo FROM hospital")
# hospital_rows = sql.fetchall()
# Get the current search path

# Retrieves the schema name to avoid hardcoding
schema = config_warehouse['database']
for row in join:
    # hid = row.get('hid')
    query = f"SELECT * FROM {schema}.hospital WHERE id=%s"  # Checks if hid is already saved in hospital table
    postgres.execute(query, (int(row[41]),))
    result = postgres.fetchone()
    if result is None:  # If hid don't exist, insert the record
        query= (f"INSERT INTO {schema}.hospital "
                "(id, name, county, planning_area, control_type, type, phone, address, city, zipcode, ceo)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        values = (row[41], row[42], row[43], row[44], row[45], row[46], row[47], row[48], row[49], row[50], row[51])
        postgres.execute(query,values)

# Insert data from expenses
# sql.execute("SELECT operational, professional FROM expenses")
# expenses_rows = sql.fetchall()

for row in join:

    query = f"SELECT id FROM {schema}.expenses WHERE id = %s"  # Query to check for duplicates
    postgres.execute(query, ((int(row[9]),),))
    result = postgres.fetchone()

    if result is None:
        query= (f"INSERT INTO {schema}.expenses "
                "(id,operational, professional)"
                "VALUES (%s, %s, %s)")
        values = (row[9], row[10], row[11])
        postgres.execute(query, values)
warehouse.commit()



# Inserting data from discharges
# sql.execute("SELECT medicare_traditional, medicare_care, medi_traditional, medi_care FROM discharges")
# discharges_rows = sql.fetchall()

for row in join:

    query = f"SELECT id FROM {schema}.discharges WHERE id = %s"  # Query to check for duplicates
    postgres.execute(query, ((int(row[4]),),))
    result = postgres.fetchone()

    if result is None:
        query= (f"INSERT INTO {schema}.discharges "
                "(id,medicare_traditional, medicare_care, medi_traditional, medi_care)"
                "VALUES (%s, %s, %s, %s, %s)")
        values = (row[4], row[5], row[6], row[7], row[8])
        postgres.execute(query, values)
warehouse.commit()

# Insert data from Outpatient Revenue
# sql.execute("SELECT medicare_traditional, medicare_care, medi_traditional, medi_care FROM outpatient_revenue")
# outpatient_rows = sql.fetchall()

for row in join:

    query = f"SELECT id FROM {schema}.outpatient_revenue WHERE id = %s"  # Query to check for duplicates
    postgres.execute(query, ((int(row[17]),),))
    result = postgres.fetchone()

    if result is None:
        query = (f"INSERT INTO {schema}.outpatient_revenue"
                 "(id,medicare_traditional, medicare_care, medi_traditional, medi_care)"
                 "VALUES (%s, %s, %s, %s, %s)")
        values = (row[17], row[18], row[19], row[20], row[21])
        postgres.execute(query, values)
warehouse.commit()



# Insert data from Inpatient Revenue
# sql.execute("SELECT medicare_traditional, medicare_care, medi_traditional, medi_care FROM inpatient_revenue")
# inpatient_rows = sql.fetchall()

for row in join:

    query = f"SELECT id FROM {schema}.inpatient_revenue WHERE id = %s"  # Query to check for duplicates
    postgres.execute(query, ((int(row[12]),),))
    result = postgres.fetchone()

    if result is None:
        query = (f"INSERT INTO {schema}.inpatient_revenue"
                 "(id, medicare_traditional, medicare_care, medi_traditional, medi_care)"
                 "VALUES (%s, %s, %s, %s, %s)")
        values = (row[12], row[13], row[14], row[15], row[16])
        postgres.execute(query, values)
warehouse.commit()



# # Inserting data from patient_days
# sql.execute("SELECT medicare_traditional, medicare_care, medi_traditional, medi_care FROM discharges")
# patient_days_rows = sql.fetchall()

for row in join:
    query = f"SELECT id FROM {schema}.patient_days WHERE id = %s"  # Query to check for duplicates
    postgres.execute(query, ((int(row[22]),),))
    result = postgres.fetchone()

    if result is None:
        query = (f"INSERT INTO {schema}.patient_days"
                 "(id, medicare_traditional, medicare_care, medi_traditional, medi_care)"
                 "VALUES (%s, %s, %s, %s, %s)")
        values = (row[22], row[23], row[24], row[25], row[26])
        postgres.execute(query, values)
warehouse.commit()

# Inserting data from report
# sql.execute("SELECT year, quarter, start_date, end_date FROM report")
# report_rows = sql.fetchall()

for row in join:
    query =(f"SELECT year, quarter FROM {schema}.report WHERE id = %s")
    postgres.execute(query, ((int(row[36]),),))
    result = postgres.fetchone()
    if result is None:   # If no duplicates exist, insert the row into the report table
        query = (f"INSERT INTO {schema}.report"
                 "(id, year, quarter, start_date, end_date)"
                 "VALUES (%s, %s, %s, %s, %s)")
        values = (row[36], row[37], row[38], row[39], row[40])
        postgres.execute(query, values)

# Inserting data from staff
sql.execute("SELECT id, hid, productive_hours_per_patient, productive_hours, position FROM staff")
staff_rows = sql.fetchall()

for row in staff_rows:
    query = f"SELECT id FROM {schema}.staff WHERE id = %s"  # Query to check for duplicates
    postgres.execute(query, ((int(row[0]),),))
    result = postgres.fetchone()

    if result is None:  # If no duplicates exist, insert the row into the staff table
        query = (f"INSERT INTO {schema}.staff "
                 "(id, hid, productive_hours_per_patient, productive_hours, position)"
                 "VALUES (%s, %s, %s,%s,%s)")
        postgres.execute(query, row)
warehouse.commit()


# Inserting data from utilization
# sql.execute("SELECT available_beds, staffed_beds, license_beds FROM utilization")
# utilization_rows = sql.fetchall()

for row in join:
    query = f"SELECT id FROM {schema}.utilization WHERE id = %s"  # Query to check for duplicates
    postgres.execute(query, ((int(row[27]),),))
    result = postgres.fetchone()

    if result is None:
        query = (f"INSERT INTO {schema}.utilization "
                    "(id,available_beds, staffed_beds, license_beds)"
                    "VALUES (%s, %s, %s, %s)")
        values = (row[27], row[28], row[29], row[30])
        postgres.execute(query, values)
warehouse.commit()

# Inserting data from visits
# sql.execute("SELECT medicare_traditional, medicare_care, medi_traditional, medi_care FROM visits")
# visits_rows = sql.fetchall()

for row in join:
    query = f"SELECT id FROM {schema}.visits WHERE id = %s"  # Query to check for duplicates
    postgres.execute(query, ((int(row[31]),),))
    result = postgres.fetchone()

    if result is None:
        query = (f"INSERT INTO {schema}.visits "
                    "(id, medicare_traditional, medicare_care, medi_traditional, medi_care)"
                    "VALUES (%s, %s, %s, %s, %s)")
        values = (row[31], row[32], row[33], row[34], row[35])
        postgres.execute(query, values)
warehouse.commit()

# Insert data to report content

for row in join:
    query = f"SELECT id FROM {schema}.report_content WHERE id = %s"  # Query to check for duplicates
    postgres.execute(query, ((int(row[0]),),))
    result = postgres.fetchone()
    if result is None:
        query = (f"INSERT INTO {schema}.report_content"
                 "(id,current_status, rid,hid, outid, visid, expid, inid, utilid, patid, disid)"
                 "VALUES (%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)")
        values = (row[0], row[1], row[2], row[3],row[17], row[31], row[9], row[12],row[27],row[22], row[4])
        postgres.execute(query, values)
warehouse.commit()