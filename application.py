from flask import Flask,request, Response
import requests
from requests.auth import HTTPBasicAuth
import json
import asyncio
import ast
#from azure.eventhub.aio import EventHubProducerClient
#from azure.eventhub import EventData
import time 
import datetime
from multiprocessing import Pool
import pandas as pd
from sqlalchemy import create_engine
import sys
import pyodbc
import urllib
  

whiteList=[
    "accounts",
    "bankAccounts",
    "companyInformation",
    "countriesRegions",
    "currencies",
    "customerPaymentJournals",
    "customers",    
    "dimensions",
    "employees",
    "generalLedgerEntries",
    "itemCategories",
    "items",
    "itemVariants",
    "journalLines",
    "journals",
    "paymentMethods",
    "paymentTerms",
    "projects",
    "purchaseInvoices",
    "salesCreditMemos",
    "salesInvoices",
    "salesOrders",
    "salesQuotes",
    "shipmentMethods",
    "unitsOfMeasure",
    "vendors"
    ]

subscriptionURL = "https://api.businesscentral.dynamics.com/v2.0/4b201fa5-e351-45de-8568-9066c14b8cc2/BC_Test/api/v1.0/subscriptions"


bc_user = "ADMIN"
bc_pw = "cl2S7OGj05fcT/eTwZUmdzZwMYzrKpp+VO1PIdy1KGo="

db_user = "svc_bi"
db_pw = "90u1hkd!8sl#"
db_server = "d4d.database.windows.net"
db_subdomain = "d4d"
db_name = "AnalyticWorkspace"

params = urllib.parse.quote_plus(
    r'Driver={ODBC Driver 17 for SQL Server};Server=tcp:d4d.database.windows.net,1433;Database=AnalyticWorkspace;Uid=svc_bi;Pwd=90u1hkd!8sl#;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    )
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)

engine_azure = create_engine(conn_str,echo=True)

def update_subscription():
    
    r = requests.get(subscriptionURL, auth= HTTPBasicAuth(bc_user,bc_pw))
    data = r.json()
    for sub in data['value']:
        expDate = sub["expirationDateTime"]
        expDate = time.mktime(datetime.datetime.strptime(expDate, "%Y-%m-%dT%H:%M:%SZ").timetuple())
        tdDate = time.mktime(datetime.datetime.now().timetuple())

        if (expDate - tdDate) < 86400:
            body = {
                "notificationUrl": sub["notificationUrl"],
                "resource": sub["resource"],
                "clientState": sub["clientState"]
            }
            headers = {
                "Content-Type" : "application/json",
                "If-Match" : "*"
            }
            print(" ")
            print(body)
            r = requests.patch(url = subscriptionURL + "('" + sub["subscriptionId"] + "')",data=json.dumps(body), auth= HTTPBasicAuth(bc_user,bc_pw), headers=headers)
            print(" ")
            print(r.status_code)
            print(r.text)

def create_subscription():   

    for entity in whiteList:
        body = {
            "notificationUrl": "https://whdemoapp.azurewebsites.net/webhook/"+entity,
            "resource": "https://api.businesscentral.dynamics.com/v2.0/4b201fa5-e351-45de-8568-9066c14b8cc2/BC_Test/api/v1.0/companies(5fc4a8c4-89d1-ea11-bb6c-000d3a2beb23)/"+entity,
            "clientState": "SomeSharedSecretForTheNotificationUrl"
        }
        headers = {
            "Content-Type" : "application/json"
        }
        r = requests.post(url=subscriptionURL, data=json.dumps(body), auth=HTTPBasicAuth(bc_user,bc_pw), headers=headers)
        print(" ")
        print(r.status_code)
        print(r.text)
        
'''
def eventHub_connection(jsonReq):
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://wheventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=IfuP+TSQiaIJpNzvkgR8S9/Wv765z2F0wNtpI5iUzkE=", eventhub_name="whtest")

    print(jsonReq)
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        for change in jsonReq['value']:
            print(change['resource'])
            data = requests.get(change['resource'], auth= HTTPBasicAuth(bc_user,bc_pw))
            print(data)
            newdata = data.content.decode("utf-8")
            
            event_data_batch.add(EventData({
                "data":newdata,
                "changeType":change['changeType'],
                "modifiedDate":change['lastModifiedDateTime']
            }))


        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

'''
def normalize_dict(df):
    try:
        newdata = pd.json_normalize(df)
        normalizeddf = pd.DataFrame.from_dict(newdata)

    except Exception as e:
        print("Fehler beim normalisieren der Daten: ")
        print(e)
        print("normalized dict: ")
        print(newdata)
        print("pandas Dataframe: ")
        print(normalizeddf)
        sys.stdout.flush()
    return normalizeddf

def write_database(df, entity, constructionType):
    try:
        df.to_sql(name = entity, con = engine_azure, schema = "webhook", if_exists = constructionType, index = False)
        print("Upload to DB successfull")
        sys.stdout.flush()
    except Exception as e :
        print("Upload to DB not successfull")
        print(e)
        print("DataFrame columns: ")
        print(df.columns)
        print("Shape: ")
        print(df.shape)
        sys.stdout.flush()

def process_json(jsonReq,entity):
    for change in jsonReq['value']:
        '''
        print("Link zur geänderten Ressource: ")
        print(change['resource'])
        print(" ")
        sys.stdout.flush()
        '''
        data = requests.get(change['resource'], auth= HTTPBasicAuth(bc_user,bc_pw))

        newdata = data.json()
        '''
        print(type(newdata))
        sys.stdout.flush()

        print("Erhaltene Daten: ")
        print(newdata)
        print(" ")
        sys.stdout.flush()
        '''
        changes = normalize_dict(newdata)

        changes["changeType"] = change['changeType']
        changes["entity"] = entity

        '''
        print("Erhaltene Daten als pandas data frame + meta data: ")
        print(changes)
        print(changes.columns)
        print(" ")
        sys.stdout.flush()
        '''

        write_database(df = changes, entity = entity+"_delta", constructionType="append")

        
    sys.stdout.flush()

def init_database():
    r = requests.get(url = subscriptionURL, auth= HTTPBasicAuth(bc_user,bc_pw))
    data = r.json()
    for sub in data['value']:
        resourceUrl = sub['resource']
        initData = requests.get(url= resourceUrl, auth= HTTPBasicAuth(bc_user,bc_pw))
        normalizedInitData = pd.DataFrame()

        for entrie in initData.json()['value']:
            normalizedEntrie = normalize_dict(entrie)
            normalizedInitData = normalizedInitData.append(normalizedEntrie)

        normalizedInitData.rename(columns={"@odata.etag": "odata.etag" }, inplace= True)
        '''
        print("Normalized Dataframe: ")
        print(normalizedInitData)
        print(normalizedInitData.columns)
        print(normalizedInitData.shape)
        sys.stdout.flush()
        '''

        

        entity = resourceUrl.split("/")[-1]
        write_database(df = normalizedInitData, entity = entity+"_bestand", constructionType="replace")



    

app = Flask(__name__)
_pool = Pool(processes=4)



@app.route("/")
def hello():
    return Response(response="Hello World", status=200)

@app.route("/init")
def init():
    f = _pool.apply_async(func = init_database)
    return Response(response="Initiating database", status=200)

@app.route("/create")
def create():
    f = _pool.apply_async(func = create_subscription)
    return Response(response="Webhook creation initated", status=200)

@app.route("/update")
def update():
    f = _pool.apply_async(func = update_subscription)
    return Response(response="Webhook update initated", status=200)

@app.route("/webhook/<entity>", methods=["POST"])
def webhook(entity):
    print(entity)
    if "validationToken" in request.args:
        validationToken = request.args.get("validationToken")
        response = Response(response=validationToken, status=200)
    else:
        print("worked")
        f = _pool.apply_async(func = process_json, args=[request.json,entity])
        response = Response(status=200)

    return response

@app.route("/webhook/crm/<entity>", methods=["POST"])
def webhook_crm(entity):
    print(entity)
    print(request.data)
    print(request.json)
    response = Response(status=200)
    return response


