# This is a sample code for moving data from Redshift to Google Sheets
# This code uses the parsons package which can be downloaded using pip install parsons
# Parsons documentation and setup guides can be found here: https://move-coop.github.io/parsons/html/stable/training_guides/getting_set_up.html

from parsons import Table, Redshift, VAN, GoogleSheets
import json
import os
import datetime
import logging


# Setup Redshift credentials: update using your own credentials
rs = Redshift(username='my_username', password='my_password', host='my_host',
              db='my_db', port='0000')

# Setup GoogleSheets API credentials: to get your json file follow directions here https://move-coop.github.io/parsons/html/stable/google.html#google-sheets
credential_filename = 'google_drive_service_credentials.json'
credentials = json.load(open(credential_filename))
sheets = GoogleSheets(google_keyfile_dict=credentials)


# Create your SQL query for your data to push to Gsheets
sql_query = f'''
select *
from schema.table
limit 10
'''
data = rs.query(sql_query)


# Before pushing your data, make your Spreadsheet editable by anyone with the link OR share the spreadsheet with your credentials' email
# To share with your credentials' email go to your Google Developer Console, find the credentials you're using, and on the details page copy the email provided. 

# Now you can push your data into the Spreadsheet and Sheet of your choice
# Your spreadsheet_id is in your Gsheets URL
sheets.overwrite_sheet(spreadsheet_id='1vCixSj26VGVf_w8bJcEPbASNfDs',
                       worksheet='sheet1',
                       table=data,
                       user_entered_value=False)
