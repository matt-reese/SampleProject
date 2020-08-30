import pandas as pd
import psycopg2
from psycopg2 import sql

health_facility_df = pd.read_csv('https://health.data.ny.gov/api/views/vn5v-hh5r/rows.csv?accessType=DOWNLOAD')
hospital_general_df = pd.read_csv('https://data.medicare.gov/api/views/xubh-q36u/rows.csv?accessType=DOWNLOAD&sorting=true', dtype = {'ZIP Code': 'object'})
medicaid_program_payments_df = pd.read_csv('https://health.data.ny.gov/api/views/6ky4-2v6j/rows.csv?accessType=DOWNLOAD')

row_query1 = 'SELECT COUNT(*) FROM sampleproject.health_facility_information'
row_query2 = 'SELECT COUNT(*) FROM sampleproject.hospital_information'
row_query3 = 'SELECT COUNT(*) FROM sampleproject.bmedicaid_ehr_provider_payments'
col_query1 = 'SELECT COUNT(*) FROM information_schema.columns WHERE table_name = \'health_facility_information\''
col_query2 = 'SELECT COUNT(*) FROM information_schema.columns WHERE table_name = \'hospital_information\''
col_query3 = 'SELECT COUNT(*) FROM information_schema.columns WHERE table_name = \'medicaid_ehr_provider_payments\''

conn = psycopg2.connect(dbname='sampleproject', user='', host='localhost', password='')
cur = conn.cursor()
cur.execute(sql.SQL(row_query1))
health_facility_row_count = cur.fetchone()

cur.execute(sql.SQL(row_query2))
hospital_row_count = cur.fetchone()

cur.execute(sql.SQL(row_query3))
medicaid_payments_row_count = cur.fetchone()

cur.execute(sql.SQL(col_query1))
health_facility_col_count = cur.fetchone()

cur.execute(sql.SQL(col_query2))
hospital_col_count = cur.fetchone()

cur.execute(sql.SQL(col_query3))
medicaid_payments_col_count = cur.fetchone()

print('Health Facility Information file has ' + str(health_facility_df.shape[0]) + ' rows and ' + str(health_facility_df.shape[1]) + ' columns.')
print('health_facility_information table has ' + str(health_facility_row_count[0]) + ' rows and ' + str(health_facility_col_count[0]) + ' columns.')
if health_facility_df.shape[0] == health_facility_row_count[0] and health_facility_df.shape[1] == health_facility_col_count[0]:
    print('SUCCESS! The shape of the data in the Health Facility Information file conforms to the table\'s generated column and row count.\n\n')
else:
    print('The shape of the data in the Health Facility Information file does not conform to the table\'s generated column and row count.\n\n')

print('Hospital Information file has ' + str(hospital_general_df.shape[0]) + ' rows and ' + str(hospital_general_df.shape[1]) + ' columns.')
print('hospital_information table has ' + str(hospital_row_count[0]) + ' rows and ' + str(hospital_col_count[0]) + ' columns.')
if hospital_general_df.shape[0] == hospital_row_count[0] and hospital_general_df.shape[1] == hospital_col_count[0]:
    print('SUCCESS! The shape of the data in the Hospital Information file conforms to the table\'s generated column and row count.\n\n')
else:
    print('The shape of the data in the Hospital Information file does not conform to the table\'s generated column and row count.\n\n')

print('Medicaid EHR Provider Payments file has ' + str(medicaid_program_payments_df.shape[0]) + ' rows and ' + str(medicaid_program_payments_df.shape[1]) + ' columns.')
print('medicaid_ehr_provider_payments table has ' + str(medicaid_payments_row_count[0]) + ' rows and ' + str(medicaid_payments_col_count[0]) + ' columns.')
if medicaid_program_payments_df.shape[0] == medicaid_payments_row_count[0] and medicaid_program_payments_df.shape[1] == medicaid_payments_col_count[0]:
    print('SUCCESS! The shape of the data in the Medicaid EHR Provider Payments file conforms to the table\'s generated column and row count.\n\n')
else:
    print('The shape of the data in the Health Facility Information file does not conform to the table\'s generated column and row count.\n\n')
