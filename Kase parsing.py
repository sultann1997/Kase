import requests
from bs4 import BeautifulSoup
import csv
from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import os
import pickle
import smtplib, ssl

df_test = pd.DataFrame() #creating Empty DataFrame

#Preparing email to automatically send notifications
from email.mime.text import MIMEText
import base64
from __future__ import print_function
import os.path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/gmail.send'] #Scopes that will be allowed by the sender (in this case to send messages from my testing email)

#Creating functions to create and send an email
def create_message(sender, to, subject, message_text):
  message = MIMEText(message_text)
  message['to'] = to 
  message['from'] = sender 
  message['subject'] = subject
  raw_message = base64.urlsafe_b64encode(message.as_string().encode("utf-8"))
  return {
    'raw': raw_message.decode("utf-8")
  }

def send_message(service, user_id, message):
  try:
    message = service.users().messages().send(userId=user_id, body=message).execute()
    print('Message Id: %s' % message['id'])
    return message
  except Exception as e:
    print('An error occurred: %s' % e)
    return None

# Check if the messaging works
# kase_update = send_message(service=build('gmail', 'v1', credentials=Credentials.from_authorized_user_file('token.json', SCOPES)), 
# user_id='sultan.python.tests@gmail.com', 
# message=create_message('sultan.python.tests@gmail.com', 'sultann1997@gmail.com', 'Kase Update', 'Kase DataFrame has been updated'))


URL = 'https://kase.kz/ru/documents/marketvaluation/'

if requests.get(URL).status_code == 200:
    print('Connected')
else:
    print("Could not connect into server")

soup = BeautifulSoup(requests.get(URL).text, features='html.parser')
items = soup.find_all('div', {'id':'a2021'})

#function to donwload and unzip files
def download_unzip(url, extract_to='.'):
    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=extract_to)


for div in soup.find_all('div', {'id':'a2021'}):
    for li in div.find_all('li'):
        a = li.find('a')
        #check if the file was already downloaded in the common directory
        if a['href'].replace(r'/files/market_valuation/ru/2021/', '') in os.listdir(r'C:\Users\Sultan\Downloads\Big Data downloads\Kase zips check2'):
            pass
        else:
            url_zip = 'https://kase.kz/' + a['href']
            #create directories for each zipfile to be extracted in
            current_dir = os.path.abspath(r'C:\Users\Sultan\Downloads\Big Data downloads\Kase zips check2'+'\\'+a['href'].replace(r'/files/market_valuation/ru/2021/', ''))
            try: os.makedirs(current_dir)
            except FileExistsError:
                pass
            download_unzip(url_zip, current_dir)
            #zipfile contains txt and xlsx files, we take xlsx files
            excel_file = [i for i in os.listdir(current_dir) if i.endswith('xlsx')]
            try: 
                temp_df = pd.read_excel(current_dir +'\\'+ excel_file[0], engine='openpyxl')
                #DataFrame automatically takes first row as headers, whereas our headers are on the third row
                headers = temp_df.iloc[1]
                temp_df = temp_df[3:]
                temp_df.columns = headers
                temp_df.drop(temp_df.columns[0], axis=1, inplace=True)
                temp_df['File'] = excel_file[0]
                df_test = df_test.append(temp_df)
                df_test.reset_index(drop=True, inplace=True)
                #sending email in case if the dataframe has been updated
                send_message(service=build('gmail', 'v1', 
                credentials=Credentials.from_authorized_user_file('token.json', SCOPES)), 
                user_id='sultan.python.tests@gmail.com', 
                message=create_message('sultan.python.tests@gmail.com', 'sultann1997@gmail.com', 'Kase Update', 'Kase DataFrame has been updated'))
            except (IndexError, AttributeError):
                pass

#exporting file into excel 
df_test.to_excel(r"C:\Users\Sultan\Downloads\Big Data downloads\my_excel.xls", engine='openpyxl')