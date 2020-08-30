import pandas as pd
import psycopg2
import psycopg2.extras as extras
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def merge_lists(list1, list2):
    return list(map(lambda x, y:(x,y), list1, list2))

def create_table(schema, table_name, column_info):
    query = f'CREATE TABLE {schema}.{table_name} ('
    column_list = []
    for tup in column_info:
        column_and_datatype = ' '.join(tup)
        column_list.append(column_and_datatype)
    query += ', '.join(column_list)
    return query + ')'

def execute_batch(conn, df, table, page_size=100):
    tuples = [tuple(x) for x in df.to_numpy()]
    number_of_format_values = '%s,'*len(df.columns)
    number_of_format_values = number_of_format_values[0:-1]
    query  = f'INSERT INTO {table} VALUES ({number_of_format_values})'
    cursor = conn.cursor()
    try:
        extras.execute_batch(cursor, query, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'Error: {error}')
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

# Creating the sampleproject database
conn1 = psycopg2.connect(dbname='postgres', user='', host='localhost', password='')
conn1.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur1 = conn1.cursor()
cur1.execute(sql.SQL('CREATE DATABASE sampleproject'))
cur1.close()
conn1.close()

# Creating the schema and tables in the sampleproject database
health_facility_df = pd.read_csv('https://health.data.ny.gov/api/views/vn5v-hh5r/rows.csv?accessType=DOWNLOAD')
hospital_general_df = pd.read_csv('https://data.medicare.gov/api/views/xubh-q36u/rows.csv?accessType=DOWNLOAD&sorting=true', dtype = {'ZIP Code': 'object'})
medicaid_program_payments_df = pd.read_csv('https://health.data.ny.gov/api/views/6ky4-2v6j/rows.csv?accessType=DOWNLOAD')

health_facility_column_list = [column.strip().replace(' ', '_') for column in list(health_facility_df.columns)]
hospital_general_column_list = [column.strip().replace(' ', '_') for column in list(hospital_general_df.columns)]
medicaid_program_payments_list = [column.replace('#', '').strip().replace(' ', '_') for column in list(medicaid_program_payments_df.columns)]

health_facility_datatypes = ['int', 'varchar', 'varchar', 'varchar', 'date', 'varchar', 'varchar', 'varchar',
                             'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'int', 'varchar', 'int',
                             'varchar', 'varchar', 'int', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                             'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                             'varchar', 'varchar', 'float', 'float', 'varchar']
hospital_general_datatypes = ['varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                              'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'int', 'varchar', 'int',
                              'varchar', 'int', 'varchar', 'int', 'varchar', 'int', 'varchar', 'int', 'varchar',
                              'int', 'varchar', 'int', 'varchar']
medicaid_program_payments_datatypes = ['varchar', 'varchar', 'varchar', 'varchar', 'int', 'int', 'date', 'float', 'int', 'int', 'varchar', 'varchar', 'varchar', 'varchar']

health_facility_column_info = merge_lists(health_facility_column_list, health_facility_datatypes)
hospital_general_column_info = merge_lists(hospital_general_column_list, hospital_general_datatypes)
medicaid_program_payments_column_info = merge_lists(medicaid_program_payments_list, medicaid_program_payments_datatypes)

conn2 = psycopg2.connect(dbname='sampleproject', user='', host='localhost', password='')
cur2 = conn2.cursor()
cur2.execute(sql.SQL('CREATE SCHEMA sampleproject'))
cur2.execute(sql.SQL(create_table('sampleproject', 'health_facility_information', health_facility_column_info)))
cur2.execute(sql.SQL(create_table('sampleproject', 'hospital_information', hospital_general_column_info)))
cur2.execute(sql.SQL(create_table('sampleproject', 'medicaid_EHR_provider_payments', medicaid_program_payments_column_info)))
conn2.commit()
cur2.close()
conn2.close()

# Cleaning data in dataframes
# Converting all phone numbers in the three files to use just a 10-digit format without non-numeric characters.
hospital_general_df['Phone Number'] = hospital_general_df['Phone Number'].str.replace('\(|\)|-| ', '')

# Converting all "location" columns in the three files to a (latitude, longitude) tuple format.
hospital_general_df['Location'] = hospital_general_df['Location'].str.replace('POINT ', '').str.strip()
hospital_general_df['Location'] = hospital_general_df['Location'].str.replace(' ', ', ')

# Converting Payment Amount column to a float format from a money format.
medicaid_program_payments_df['Payment Amount'] = medicaid_program_payments_df['Payment Amount'].str.replace('\$|,|\)| ', '')
medicaid_program_payments_df['Payment Amount'] = medicaid_program_payments_df['Payment Amount'].str.replace('\(', '-')

# Stripping all string columns
health_facility_df_obj = health_facility_df.select_dtypes(['object'])
health_facility_df[health_facility_df_obj.columns] = health_facility_df_obj.apply(lambda x: x.str.strip())

hospital_general_df_obj = hospital_general_df.select_dtypes(['object'])
hospital_general_df[hospital_general_df_obj.columns] = hospital_general_df_obj.apply(lambda x: x.str.strip())

medicaid_program_payments_df_obj = medicaid_program_payments_df.select_dtypes(['object'])
medicaid_program_payments_df[medicaid_program_payments_df_obj.columns] = medicaid_program_payments_df_obj.apply(lambda x: x.str.strip())

# Converting dataframe NaNs to Nones, which will load as NULLs in the tables
health_facility_df = health_facility_df.where(pd.notnull(health_facility_df), None)
hospital_general_df = hospital_general_df.where(pd.notnull(hospital_general_df), None)
medicaid_program_payments_df = medicaid_program_payments_df.where(pd.notnull(medicaid_program_payments_df), None)

# Loading data into tables
conn = psycopg2.connect(dbname='sampleproject', user='', host='localhost', password='')
execute_batch(conn, health_facility_df, 'sampleproject.health_facility_information')
execute_batch(conn, hospital_general_df, 'sampleproject.hospital_information')
execute_batch(conn, medicaid_program_payments_df, 'sampleproject.medicaid_EHR_provider_payments')
conn.close()

# Creating indexes
conn3 = psycopg2.connect(dbname='sampleproject', user='', host='localhost', password='')
cur3 = conn3.cursor()
cur3.execute(sql.SQL('CREATE INDEX health_facility_facility_id_idx ON sampleproject.health_facility_information (facility_id)'))
cur3.execute(sql.SQL('CREATE INDEX hospital_facility_id_idx ON sampleproject.hospital_information (facility_id)'))
cur3.execute(sql.SQL('CREATE INDEX medicaid_payments_provider_npi_idx ON sampleproject.medicaid_ehr_provider_payments (provider_npi)'))
cur3.execute(sql.SQL('CREATE INDEX medicaid_payments_payee_npi_idx ON sampleproject.medicaid_ehr_provider_payments (payee_npi)'))
conn3.commit()
cur3.close()
conn3.close()
