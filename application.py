from flask import Flask,request, Response
import pip
import json
import os, uuid

from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)



app = Flask(__name__)

@app.route("/")
def hello():
    
    return Response(response="Hello World", status=200)

@app.route("/webhook", methods=["POST"])
def webhook():
    with open("data.txt", "w") as outfile:
        json.dump(request.json,outfile)
    validationToken = request.args.get("validationToken")
    return Response(response=validationToken, status=200)

