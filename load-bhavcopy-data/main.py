import base64
import re
from google.cloud import datastore
from datetime import datetime, timedelta
from zipfile import ZipFile
from google.cloud import storage
from google.cloud.storage import Blob
import logging

# This function recieves filename and use batch loading to load the data.
def store_data(fname):
    
    # Extract date from filename.  
    # file_date variable is inserted into the database along with other 12 fields in the file.
    extracted_date = re.search(r'([eE][qQ])(\d\d\d\d\d\d)', fname).group(2)
    file_date = datetime.strptime(extracted_date,'%d%m%y')

    # Download file from cloud storage and unzip it    
    storage_client = storage.Client(project='bhavcopy')
    bucket = storage_client.get_bucket('bhavcopy-store')
    blob = bucket.blob(fname)
    blob.download_to_filename('/tmp/'+fname)
    zip_file = ZipFile('/tmp/'+fname, 'r')
    zip_file.extractall('/tmp/')
                              
    # Set the client and open the file
    client = datastore.Client(project='bhavcopy')
    f = open('/tmp/'+ fname.split('_')[0] + '.CSV')
    data = f.readlines()
    header_read = False
    
    # Another important variable.  This array stores all the individual data entities. 
    # put_multi is then used to bulk commit the data, this is much faster.
    bhavcopy_rows = []

    # First line of each file is header, we collect column names from this line and then for each
    # subsequent row entity is created using file header fields as column names.
    for line in data:
        if not header_read:
            headers = line.split(',')
            header_read = True
        else:
            row_data = line.split(',')
            bhavcopy_row = datastore.Entity(client.key('DailyBhavcopy',row_data[1]+'-'+extracted_date))
            bhavcopy_row['FILE_DATE'] = file_date
            for i in range(12):
                if i in (1,2,3):
                    bhavcopy_row[headers[i]] = str(row_data[i]).strip()
                else:
                    bhavcopy_row[headers[i]] = float(row_data[i].strip())
                    
            # Do not use extend() function, it changes the entity object to string.        
            bhavcopy_rows.append(bhavcopy_row)
        
    logging.info('Collected '+str(len(bhavcopy_rows))+' rows from '+fname)
    
    # Bulk insert has limied to maximum 500 rows, hence this block
    for i in range(0,len(bhavcopy_rows),400):
        client.put_multi(bhavcopy_rows[i:i+400]) 
        logging.info(fname+": "+ "Storing rows from " + str(i) +" to "+ str(i+400))               

# Storage object notification gurantees atleast once, this means pubsub is notifed 1 or 10 or 100 times.  
# So need to ensure that data from same file is not inserted multiple times. 
# Transaction block is used to ensure only one thread gets to load the file.
def lock_and_load(fname):
    client = datastore.Client(project='bhavcopy')
    load_data = False
    try:
        with client.transaction():
            key = client.key('RecievedFiles', fname)
            row = client.get(key)
            if (row is not None) and ('status' not in row):
                row['status'] = 'Datastore'
                client.put(row)
                load_data = True        
        if load_data:
            store_data(fname)
            logging.info('Data loaded in datastore for ' + fname)
        else:
            logging.info('No database entry for file or it is already loaded: ' + fname) 
    except Exception as e:
        logging.info("Cannot start transaction for loading file "+ fname + ".Recieved error:" + str(e))

# Main function to kick start loading. This only retrieves filename from the message            
def load_bhavcopy_data(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')   
    fname = event['attributes']['objectId']
    logging.info(fname+': '+ pubsub_message)
    logging.info(fname+': '+str(event['attributes']))
    lock_and_load(fname)

# Use this when you just want function to execute but do nothing when testing or debugging.        
def do_nothing(event, context):
    logging.info("Exiting without loading data")
                              
