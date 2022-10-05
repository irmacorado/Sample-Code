#Code to load data from Redshift to VAN as a saved list

# load the necessary packages
from parsons import Table, Redshift, VAN
import os
import datetime
from canalespy import setup_environment, logger

# Setup enviroment/keys
setup_environment()
rs = Redshift()
van_key = os.environ['VAN_PASSWORD']
ea = VAN(api_key=van_key, db='MyVoters') #check this

# Set parameters
van_folder_id = 2390 #FOLDER ID
van_list_name = 'Latinx List'


# Define Function that 1) creates the table using a parameter; and 2) pushes that table as a list in EA
def table_to_EA(min_target_score):
  
  # Get Table using SQL
  table_query = f'''
  select vb_smartvan_id, latino_combined_field_score
  from mijente.universe
  where record_state = 'GA'
  and vb_smartvan_id is not null
  and latino_combined_field_score >= %s
  '''
  table = rs.query(table_query, parameters=[min_target_score])
  logger.info(f"{table_query.num_rows} rows found.")

  # Push to EA
  if table.num_rows > 0:
    table_cols = table[0]
    logger.info(f"Header 1: {table_cols[0]} -- Header 2: {table_cols[1]} ")
    
    van_list_name += " > "
    van_list_name += str(min_target_score)
    
    try:
      upload_saved_list_rest(tbl=table, 
                             url_type=S3, 
                             folder_id=van_folder_id, 
                             list_name=van_list_name,
                             description='', 
                             callback_url='http://', #fix
                             columns=table_cols, 
                             id_column='vb_smartvan_id', 
                             delimiter='csv', 
                             header=True, 
                             quotes=False, 
                             overwrite=1)
      logger.info(f"Loaded table for {van_list_name}.")
    except:
      logger.info(f"Failed to table for {van_list_name}.")
      
# Loop for each min target score up to 14 using the new function
min_target_score = 1
for i in range(14):
  table_to_EA(min_target_score=min_target_score)
  min_target_score+=1
