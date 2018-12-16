import base64
import re
from datetime import datetime, timedelta, date
import requests
import logging
from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud import datastore
import zipfile

# PubSub can send multiple notifications to cloud functions
# Below code ensure that only one process downloads the files and rest just exit. 
# We utilize datastore transaction to manage this.
def check_if_already_stored(fname):
    already_downloaded = True
    client = datastore.Client(project='bhavcopy')
    with client.transaction():
        key = client.key('RecievedFiles', fname)
        result = client.get(key)
        if result is None:
            entity = datastore.Entity(key=key)
            entity['file_name'] = fname
            client.put(entity)
            already_downloaded = False
            logging.info('Created new entry for ' + fname)
        else:
            logging.info('Received duplicate message for ' + fname)
        return already_downloaded   

# Code generates holiday dictionary to check against public holidays before attempting to download the file in main function.,   
def create_holiday_dict():
    holiday_dict = {}
    client = datastore.Client(project='bhavcopy')
    query = client.query(kind='Trading-Holidays')
    for row in query.fetch():
        holiday_date_strf = row['Date'].strftime('%d%m%y')
        holiday_dict[holiday_date_strf] = holiday_date_strf
    return holiday_dict

# Functions checks if its off day or not (Weekend or Holiday). weekday() returns monday as 0.
def check_weekend_holiday(new_date,holiday_dict):
    isoffday = False
    if new_date.weekday() == 5 or new_date.weekday() == 6 or (new_date.strftime('%d%m%y') in holiday_dict):
        isoffday = True
    return isoffday    

# This functions downloads the file available for a given date or next file available on next available date if current is offday
# Returns None, if the file cannot be downloaded for any reason.
def check_and_download(new_date,holiday_dict,base_url):
    file_downloaded = False
    new_fname = 'None'
    while (not file_downloaded) and (new_date < datetime.today()):
        while check_weekend_holiday(new_date, holiday_dict):
            new_date = new_date + timedelta(days=1)
        new_fname = 'EQ' + new_date.strftime('%d%m%y') +'_CSV.ZIP'
        r = requests.get(base_url + new_fname, allow_redirects=False)
        if r.status_code == 200:
            open('/tmp/'+new_fname, 'wb').write(r.content)
            if zipfile.is_zipfile('/tmp/'+new_fname):
            	file_downloaded = True
            	logging.info(new_fname +' downloaded in /tmp directory')
            else:
                new_date = new_date + timedelta(days=1)
                logging.warn(new_fname +' is not valid zipfile - content maybe 404 redirect')
        else:
            new_date = new_date + timedelta(days=1)
            logging.warn(new_fname +' could not be downloaded')        
    return (file_downloaded, new_fname)    

# This is the main function, which recieves message from pubsub. 
# It then attempts to download file for the "next date" as the message is only recieved when current file is downloaded and stored in
# google storage.
def download_bhavcopy(event, context):
    
    holiday_dict = create_holiday_dict()
    logging.info('Dictionary created for '+str(len(holiday_dict))+ ' holidays')
    
    base_url = 'https://www.bseindia.com/download/BhavCopy/Equity/'
    
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')   
    print(pubsub_message)
    print(event['attributes'])
    fname = event['attributes']['objectId']
    
    extracted_date = re.search(r'([eE][qQ])(\d\d\d\d\d\d)', fname).group(2)
    new_date = datetime.strptime(extracted_date,'%d%m%y') + timedelta(days=1)   
        
    file_downloaded_locally , new_fname = check_and_download(new_date,holiday_dict,base_url)
    try:
        if file_downloaded_locally and (not check_if_already_stored(new_fname)):  
            client = storage.Client(project='bhavcopy')
            bucket = client.get_bucket('bhavcopy-store')
            blob = Blob(new_fname, bucket)
            with open('/tmp/'+new_fname, 'rb') as my_file:
              blob.upload_from_file(my_file) 
    except Exception as e:
        logging.info('Not Downloaded: Cloud function exiting without storing file for date: '+ str(new_date) +
                    '.Received error: ' + str(e))
  
