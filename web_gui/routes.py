from flask import Blueprint, render_template, current_app, redirect

import pdb

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def home():
    return redirect("/queue", code=302)
    
@main_bp.route('/queue')
def request_queue():
    mongo = current_app.config['MONGO']
    data = mongo.db.request_queue.find({"status": "pending"}).sort("payload.next_update_at", 1)
    return render_template('pending_requests.html', data=data)

@main_bp.route('/completed')
def completed_requests():
    mongo = current_app.config['MONGO']
    data = mongo.db.request_queue.find({"status": "completed"}).sort("payload.last_updated_at", -1)
    return render_template('completed_requests.html', data=data)

@main_bp.route('/archived')
def archived_pages():
    mongo = current_app.config['MONGO']
    data = mongo.db.archived_pages.find().sort("payload.archived_at", -1)
    return render_template('archived_pages.html', data=data)

@main_bp.route('/data')
def data_items():
    mongo = current_app.config['MONGO']
    data = mongo.db.item_store.find().sort("parsed_at", -1)
    return render_template('crawled_data.html', data=data)
