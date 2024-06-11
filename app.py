from flask import Flask, request, jsonify, render_template
from flask_pymongo import PyMongo
#from flask_restful import Api, Resource
from pymongo import MongoClient

from datetime import datetime, timedelta
import json
import logging
import os
import pdb

logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)

config_path = os.getenv("APP_CONF", "config.json")
if not os.path.exists(config_path):
    print(f"Configuration file {config_path} not found. Please create it from config.json.example.")
with open(config_path) as f:
    config = json.load(f)
app.config.update(config)
app.config["MONGO_URI"] = f"mongodb://{app.config['DB_HOST']}:{app.config['DB_PORT']}/{app.config['DB_NAME']}"

mongo = PyMongo(app)

@app.route('/')
def home():
    data = mongo.db.request_queue.find({"status": "pending"}).sort("payload.next_update_at", 1)
    return render_template('index.html', data=data)

@app.route('/completed')
def completed_requests():
    data = mongo.db.request_queue.find({"status": "completed"}).sort("payload.last_updated_at", -1)
    return render_template('completed_requests.gpt4.html', data=data)

if __name__ == '__main__':

    app.run()
