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
  

whiteList=[
    "accounts",
    "employees",
    "customers",
    "salesInvoices",
    "salesOrders",
    "items",
    "currencies",
    "journals",
    "projects",
    "shipmentMethods",
    "companyInformation",
    "countriesRegions",
    "customerPaymentJournals"
    ]

subscriptionURL = "https://api.businesscentral.dynamics.com/v2.0/4b201fa5-e351-45de-8568-9066c14b8cc2/BC_Test/api/v1.0/subscriptions"

#i = -1

bc_user = "ADMIN"
bc_pw = "cl2S7OGj05fcT/eTwZUmdzZwMYzrKpp+VO1PIdy1KGo="

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
        #if r.status_code == 400 :
            #if i < (len(whiteList)-1):
                #i += 1
                #print(i)
                #create_subscription(i)

        
'''
async def process_json(jsonReq):
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

def processJson(jsonReq,entity):
    print(entity)

app = Flask(__name__)
_pool = Pool(processes=4)

@app.route("/")
def hello():
    f = _pool.apply_async(create_subscription)
    #r = f.get(timeout=5)
    #create_subscription()
    #update_subscription()
    return Response(response="Hello World", status=200)

@app.route("/webhook/<entity>", methods=["POST"])
def webhook(entity):
    #global i
    print(entity)
    print(request.args.get("validationToken"))
    validationToken = request.args.get("validationToken")
    response = Response(response=validationToken, status=200)
    print(response)
    return response
    #if i < (len(whiteList)-1):
        #i += 1
        #print(i)
        #create_subscription(i)

