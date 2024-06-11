from flask import Blueprint, render_template, current_app

import pdb

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def home():
    mongo = current_app.config['MONGO']
    data = mongo.db.request_queue.find({"status": "pending"}).sort("payload.next_update_at", 1)
    return render_template('pending_requests.html', data=data)

@main_bp.route('/completed')
def completed_requests():
    mongo = current_app.config['MONGO']
    data = mongo.db.request_queue.find({"status": "completed"}).sort("payload.last_updated_at", -1)
    return render_template('completed_requests.gpt4.html', data=data)
