import requests
import argparse
from bs4 import BeautifulSoup
import json

parser = argparse.ArgumentParser()
parser.add_argument('startwebscraping')
args = parser.parse_args()
startwebscraping = args.startwebscraping 
page = requests.get(startwebscraping)

source = BeautifulSoup(page.content, 'html.parser')
computrabajo = source.find('div', attrs={'class':'gO'})

def handler(event, context):
  get_history_job(computrabajo)
  print(Trabajo Realizado)

  return {
        'headers': {'Content-Type': 'application-json'},
        'statusCode': 200,
        'body': json.dumps({"message": "Lambda Container image invoked!",
                            "event": event})
  }

def get_history_job(computrabajo):
  capture = {}
  for search_job in computrabajo.find_all('div', attrs={'class':'iO'}):                                                   
   job = search_job.find_all('a', attrs={'class':'js-o-link'})
   for clean_job in job:
    capture['Puesto'] = clean_job.text                                                                                                                                                                                      
   print(capture)

'''def get_history_business(computrabajo):
  capture = {}
  for search_job in computrabajo.find_all('div', attrs={'class':'iO'}):                                                  
   business = search_job.find_all('span', attrs={'itemprop':'name'})
   for clean_job in business:
    capture['Empresa'] = clean_job.text                                                                                                                                                                                      
   print(capture)

def get_history_address_locality(computrabajo):
  capture = {}
  for search_job in computrabajo.find_all('div', attrs={'class':'iO'}):                                                  
   address = search_job.find_all('span', attrs={'itemprop':'addressLocality'})
   for clean_job in address:
    capture['Ubicación'] = clean_job.text                                                                                                                                                                                      
   print(capture)'''