import pandas as pd
from mysql.connector.errors import Error
import sys
import mysql.connector
import os
from config import config_db

# Check arguments. Argument one must be reports folder and argument two must be staff folder
if len(sys.argv) != 3:
    print("python load.py <reports_dataset_folder> <staff_dataset_folder> Expected")
    sys.exit(1)


# Method to load the reports to their tables
def load_report(data):
    # Makes the connection with the database
    try:
        db = mysql.connector.connect(**config_db)
    except Error as e:
        print(e)
        exit(1)

    cursor = db.cursor()  # Object to execute MySQL queries
    dfReport = data
    dfReport = dfReport.fillna('')
    # Inserts data into hospital table
    for item, row in dfReport.iterrows():
        hid = row['hid']
        query = "SELECT * FROM hospital WHERE id=%s"  # Checks if hid is already saved in hospital table
        cursor.execute(query, (row['hid'],))
        result = cursor.fetchone()
        if result is None:  # If hid don't exist, insert the record
            query = ("INSERT INTO hospital "
                     "(id, name, county, planning_area, control_type, type, phone, address, city, zipcode, ceo, latest) "
                     "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

            values = (row['hid'], row['name'], row['county_name'], row['planning_area'],
                      row['type_cntrl'], row['type_hosp'], row['phone'], row['address'], row['city'],
                      row['zip_code'], row['ceo'], row['year_qtr'])
            cursor.execute(query, values)  # execute the query
            db.commit()
        # If record already exist: Update?
        else:

            if result is None or (
                   int(row['year_qtr']) > int(result[11])):  # If no record exists or new record is newer
                # Use COALESCE to decide which value to use
                # COALESCE returns the first not null value
                # Here, it receives the value from old record and new one
                # It then checks which one is not null and use it for the update
                update_query = """
                    UPDATE hospital
                    SET name = COALESCE(%s, name),
                        county = CASE WHEN %s IS NULL THEN county ELSE COALESCE(%s, county) END,
                        planning_area = CASE WHEN %s IS NULL THEN planning_area ELSE COALESCE(%s, planning_area) END,
                        control_type = CASE WHEN %s IS NULL THEN control_type ELSE COALESCE(%s, control_type) END,
                        type = CASE WHEN %s IS NULL THEN type ELSE COALESCE(%s, type) END,
                        phone = CASE WHEN %s IS NULL THEN phone ELSE COALESCE(%s, phone) END,
                        address = CASE WHEN %s IS NULL THEN address ELSE COALESCE(%s, address) END,
                        city = CASE WHEN %s IS NULL THEN city ELSE COALESCE(%s, city) END,
                        zipcode = CASE WHEN %s IS NULL THEN zipcode ELSE COALESCE(%s, zipcode) END,
                        ceo = CASE WHEN %s IS NULL THEN ceo ELSE COALESCE(%s, ceo) END,
                        latest = %s
                    WHERE id = %s;
                """

                # Determine which values should be updated
                update_values = [
                    row.get('name', result[1]),
                    row.get('county_name', result[2]),
                    row.get('county_name', result[2]),
                    # for county, use the new value if not None, otherwise use the old value
                    row.get('planning_area', result[3]),
                    row.get('planning_area', result[3]),
                    # for planning_area, use the new value if not None, otherwise use the old value
                    row.get('type_cntrl', result[4]),
                    row.get('type_cntrl', result[4]),
                    # for control_type, use the new value if not None, otherwise use the old value
                    row.get('type_hosp', result[5]),
                    row.get('type_hosp', result[5]),
                    # for type, use the new value if not None, otherwise use the old value
                    row.get('phone', result[6]),
                    row.get('phone', result[6]),
                    # for phone, use the new value if not None, otherwise use the old value
                    row.get('address', result[7]),
                    row.get('address', result[7]),
                    # for address, use the new value if not None, otherwise use the old value
                    row.get('city', result[8]),
                    row.get('city', result[8]),  # for city, use the new value if not None, otherwise use the old value
                    row.get('zip_code', result[9]),
                    row.get('zip_code', result[9]),
                    # for zipcode, use the new value if not None, otherwise use the old value
                    row.get('ceo', result[10]),
                    row.get('ceo', result[10]),  # for ceo, use the new value if not None, otherwise use the old value
                    row['year_qtr'],
                    hid
                ]

                cursor.execute(update_query, update_values)
                db.commit()

    # Inserts data into report table
    for item, row in dfReport.iterrows():

        # Kept getting an error if test csv files where divided, so this should fix it
        # This just formats the start and end date's columns
        try:
            # Try to convert the date column to the expected format
            row['start_date'] = pd.to_datetime(row['start_date'], format='%m/%d/%y').date()
            row['end_date'] = pd.to_datetime(row['end_date'], format='%m/%d/%y').date()
        except ValueError:
            # If that fails, try converting to the YYYY-MM-DD format
            row['start_date'] = pd.to_datetime(row['start_date'], format='%Y-%m-%d').date()
            row['end_date'] = pd.to_datetime(row['end_date'], format='%Y-%m-%d').date()

        year = row['year']
        qrt = row['quarter']
        rid = None  # Creates rid variable

        queryr = "SELECT * FROM report WHERE year = %s AND quarter = %s"  # Checks if there is a record with the year and quarter
        cursor.execute(queryr, (row['year'], row['quarter']))
        result2 = cursor.fetchone()
        if result2 is None:  # If the record doesn't exist, it's added
            query = ("INSERT INTO report "
                     "(year, quarter, start_date, end_date)"
                     "VALUES (%s, %s, %s, %s)")
            values = (row['year'], row['quarter'], row['start_date'], row['end_date'])
            cursor.execute(query, values)
            db.commit()
            rid = cursor.lastrowid
        else:
            rid = result2[0]  # To fix null rid
        # Inserts data into report_content table
        query = ("INSERT INTO report_content"
                 "(current_status, rid, hid)"
                 "VALUES (%s, %s, %s)")
        row['hstatus'] = row['hstatus'].lower()
        values = (row['hstatus'], rid, row['hid'])
        cursor.execute(query, values)
        db.commit()

        # Inserts data into expenses table
        query = ("INSERT INTO expenses"
                 "(operational, professional)"
                 "VALUES (%s, %s)")
        values = (row['operational_expenses'], row['professional_expenses'])
        cursor.execute(query, values)
        db.commit()

        # Inserts data into inpatient_revenue table
        query = ("INSERT INTO inpatient_revenue"
                 "(medicare_traditional, medicare_care, medi_traditional, medi_care)"
                 "VALUES (%s, %s, %s, %s)")
        values = (row['grip_medicare_tradicional'], row['grip_medicare_care'], row['grip_medi_tradicional'],
                  row['grip_medi_care'])
        cursor.execute(query, values)
        db.commit()

        # Inserts data into utilization table
        query = ("INSERT INTO utilization"
                 "(available_beds, staffed_beds, license_beds)"
                 "VALUES (%s, %s, %s)")
        values = (row['avl_beds'], row['stf_beds'], row['lic_beds'])
        cursor.execute(query, values)
        db.commit()

        # Inserts data into outpatient_content table
        query = ("INSERT INTO outpatient_revenue"
                 "(medicare_traditional, medicare_care, medi_traditional, medi_care)"
                 "VALUES (%s, %s, %s, %s)")
        values = (row['grop_medicare_tradicional'], row['grop_medicare_care'], row['grop_medi_tradicional'],
                  row['grop_medi_care'])
        cursor.execute(query, values)
        db.commit()

        # Inserts data into visits table
        query = ("INSERT INTO visits"
                 "(medicare_traditional, medicare_care, medi_traditional, medi_care)"
                 "VALUES (%s, %s, %s, %s)")
        values = (row['vis_medicare_tradicional'], row['vis_medicare_care'], row['vis_medi_tradicional'],
                  row['vis_medi_care'])
        cursor.execute(query, values)
        db.commit()

        # Inserts data into patient_days table
        query = ("INSERT INTO patient_days"
                 "(medicare_traditional, medicare_care, medi_traditional, medi_care)"
                 "VALUES (%s, %s, %s, %s)")
        values = (row['day_medicare_tradicional'], row['day_medicare_care'], row['day_medi_tradicional'],
                  row['day_medi_care'])
        cursor.execute(query, values)
        db.commit()

        # Inserts data into discharges table
        query = ("INSERT INTO discharges"
                 "(medicare_traditional, medicare_care, medi_traditional, medi_care)"
                 "VALUES (%s, %s, %s, %s)")
        values = (row['dis_medicare_tradicional'], row['dis_medicare_care'], row['dis_medi_tradicional'],
                  row['dis_medi_care'])
        cursor.execute(query, values)
        db.commit()

    cursor.close()
    db.close()


def load_staff(data):
    #   Makes the connection to the databse
    try:
        db = mysql.connector.connect(**config_db)
    except Error as e:
        print("Could not connect.")
        exit(1)

    cursor = db.cursor()

    # Inserts data into staff table
    for item, row in data.iterrows():
        hid = int(row[0])
        query = "SELECT id FROM hospital WHERE id = %s"  # Query to check if the hid is in the hospital table, if not skip it
        cursor.execute(query, (hid,))
        result = cursor.fetchone()
        if result is not None:  # If hid exist in the hospital table, insert it to staff table
            #Check for duplicates
            query = "SELECT * FROM staff WHERE hid = %s AND ABS(productive_hours_per_patient - %s) < 0.001 AND productive_hours = %s AND position = %s "  # Query to check for duplicates
            cursor.execute(query, (row['facility_id'], row['productive_hours_per_patient'],row['productive_hours'],row['position']))
            result = cursor.fetchone()
            if result is None:  # If no duplicates exist, insert the row into the staff table
                query = ("INSERT IGNORE INTO staff "
                         "(hid, productive_hours_per_patient, productive_hours, position)"
                         "VALUES (%s, %s, %s, %s)")
                values = (
                    row['facility_id'], row['productive_hours_per_patient'], row['productive_hours'], row['position'])
                cursor.execute(query, values)
                db.commit()
    cursor.close()
    db.close()


if __name__ == '__main__':

    # Saves arguments in data frames
    reports = sys.argv[1]
    staff = sys.argv[2]

    for filename in os.listdir(reports):
        if filename.endswith(".csv"):
            reportsFile = os.path.join(reports, filename)  # Save file from folder
            dfReport = pd.read_csv(reportsFile)

            # Separates the year column to a year column and a quarter column
            dfReport['year'] = dfReport['year_qtr'] // 10
            dfReport['quarter'] = dfReport['year_qtr'] % 10
            # dfReport.drop('year_qtr', axis=1, inplace=True)
            # dfReport['start_date'] = pd.to_datetime(dfReport['start_date'], format='%m/%d/%y').dt.date
            # dfReport['end_date'] = pd.to_datetime(dfReport['end_date'], format='%m/%d/%y').dt.date

            load_report(dfReport)

    for filename in os.listdir(staff):
        if filename.endswith(".csv"):
            staffFile = os.path.join(staff, filename)
            dfStaff = pd.read_csv(staffFile)
            load_staff(dfStaff)
