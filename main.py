from google.api_core.exceptions import BadRequest
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import smtplib
import string
import io
import sys

storage_client = storage.Client()
bigquery_client = bigquery.Client()

SUCCESS_BUCKET_NAME = "NAME-BUCKET-FILES-REGISTER"
SOURCE_BUCKET_NAME = "NAME-BUCKET-FILES-SOURCE"
ERROR_BUCKET_NAME = "NAME-BUCKET-FILES-WITH-ERRORS"
DUPLICATED_BUCKET_NAME = "NAME-BUCKET-FILES-DUPLICATES"

sender = "my-email-source@gmail.com"
password = "my-password"
receiver = "email-destinaty@gmail.com" 

def send_email(file_name,event):
    
  message = """An event with the file %s 
            Result: %s """ % (file_name, event)

  server = smtplib.SMTP("smtp.gmail.com",587)
  server.starttls()
  try:  
    server.login(sender,password)
    print("Logged in...")
    server.sendmail(sender,receiver,message)
    print("Email has been sent!")
  except smtplib.SMTPAuthenticationError:
    print("Unable to sign in")

def move_bucket_success(file_name):
  move_file_name = file_name
  source_bucket = storage_client.get_bucket(SOURCE_BUCKET_NAME)
  source_blob = source_bucket.blob(move_file_name)
  destination_bucket = storage_client.get_bucket(DESTINATION_BUCKET_NAME)

  source_bucket.copy_blob(source_blob, destination_bucket, move_file_name)
  message = f"""File {file_name} recorded, the file was move to success bucket"""
  source_blob.delete()
  return message

def move_file_duplicated(file_name):
  move_file_name = file_name
  source_bucket = storage_client.get_bucket(SOURCE_BUCKET_NAME)
  source_blob = source_bucket.blob(move_file_name)
  destination_bucket = storage_client.get_bucket(DUPLICATED_BUCKET_NAME)

  source_bucket.copy_blob(source_blob, destination_bucket, move_file_name)
  message = f"""File {file_name} duplicated, the file was move to duplicated bucket"""
  source_blob.delete()
  return message

def move_file_error(file_name):
  move_file_name = file_name
  source_bucket = storage_client.get_bucket(SOURCE_BUCKET_NAME)
  source_blob = source_bucket.blob(move_file_name)
  destination_bucket = storage_client.get_bucket(ERROR_BUCKET_NAME)

  source_bucket.copy_blob(source_blob, destination_bucket, move_file_name)
  message = f"""File {file_name} with error, the file was move to error bucket"""
  source_blob.delete()
  return message

def insert_into_bigquery(bucket_name,file_name):
  job_config = bigquery.LoadJobConfig(
    schema=[
      bigquery.SchemaField("COLUMN-A", "STRING"),
      bigquery.SchemaField("COLUMN-B", "STRING"),
      bigquery.SchemaField("COLUMN-C", "STRING"),
      bigquery.SchemaField("COLUMN-D", "INTEGER"),
      bigquery.SchemaField("COLUMN-E", "INTEGER"),
      bigquery.SchemaField("COLUMN-F", "INTEGER"),
      bigquery.SchemaField("COLUMN-G", "INTEGER"),
      bigquery.SchemaField("COLUMN-H", "INTEGER")
    ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
  )
  uri = "gs://%s/%s" %(bucket_name,file_name)

  try:
    load_job = bigquery_client.load_table_from_uri(uri,"project-id.dataset.table", job_config=job_config) 
    return load_job.result()
  except BadRequest as e:
    for e in load_job.errors:
      move_file_error(file_name)
      return e['message']

def compare(list_file,bucket_name,file_name):
  if file_name in list_file:
    event = move_file_duplicated(file_name)
    send_email(file_name,event)
  else:
    try:
      event = insert_into_bigquery(bucket_name,file_name)
      send_email(file_name,event)
        
      blobs = storage_client.list_blobs(SOURCE_BUCKET_NAME)
      list_file = []
      for blob in blobs:
        list_file.append(blob.name)
        if file_name in list_file: 
          move_bucket_success(file_name)
        else:
          pass    
    except ValueError:  
      print("Oops!")

def streaming(data,context):
  bucket_name = data['bucket']
  file_name = data['name']
  blobs = storage_client.list_blobs(DESTINATION_BUCKET_NAME)
  list_file = []
  for blob in blobs:
    list_file.append(blob.name)
  compare(list_file,bucket_name,file_name)