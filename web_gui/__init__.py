from .routes import main_bp

from flask import Flask, request, jsonify, render_template
from flask_pymongo import PyMongo
from pymongo import MongoClient
from markupsafe import Markup

from datetime import datetime, timedelta
import json
import logging
import os
import pdb

logging.basicConfig(level=logging.DEBUG)

def create_app():
    app = Flask(__name__)

    config_path = os.getenv("APP_CONF", "config.json")
    if not os.path.exists(config_path):
        print(f"Configuration file {config_path} not found. Please create it from config.json.example.")
    with open(config_path) as f:
        config = json.load(f)
    app.config.update(config)
    app.config['MONGO_URI'] = f"mongodb://{app.config['DB_HOST']}:{app.config['DB_PORT']}/{app.config['DB_NAME']}"
    app.config['MONGO'] = PyMongo(app)

    app.jinja_env.filters['format_key'] = jinja_filter__format_key
    app.jinja_env.filters['format_item'] = jinja_filter__format_item
    app.jinja_env.filters['format_datetime'] = jinja_filter__format_datetime

    app.register_blueprint(main_bp)
    
    return app

def jinja_filter__format_key(value):
    if value is None:
        return ""
    items = ', '.join(f'<strong>{key}</strong>: {val}' for key, val in value.items())
    return Markup(items)

def jinja_filter__format_item(value):
    items = ', '.join(f'<strong>{key}</strong>: {val}' for key, val in value.items() if key != '_type')
    return Markup(items)

def jinja_filter__format_datetime(value):
    if value is None:
        return ""
    now = datetime.now()
    today = now.date()
    
    if value.date() == today:
        formatted = value.strftime("Today, %H:%M")
    elif value.date() == today + timedelta(days=1):
        formatted = "tomorrow"
    elif value.date() == today - timedelta(days=1):
        formatted = "yesterday"
    else:
        _format = "%B %d"
        if value.year != now.year:
            _format += ", %Y"
        formatted = value.strftime(_format)
    
    full_datetime = value.strftime("%Y-%m-%d %H:%M:%S")
    return Markup(f'<span title="{full_datetime}">{formatted}</span>')
