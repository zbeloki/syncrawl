from . import utils

from flask import request, Blueprint, render_template, current_app, redirect

import pdb

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def home():
    return redirect("/queue", code=302)

@main_bp.route('/queue', methods=['GET'])
def request_queue():
    mongo = current_app.config['MONGO']
    data = mongo.db.request_queue.find({"status": "pending"}).sort("payload.next_update_at", 1)
    get_page_f = lambda e: e['payload']['page']
    data = utils.filter_by_pages(data, get_page_f, request)
    available_page_names = mongo.db.request_queue.distinct("payload.page.page_name")
    return render_template(
        'pending_requests.html',
        data=data,
        page_names=available_page_names,
        query_params=request.args,
    )

@main_bp.route('/completed')
def completed_requests():
    mongo = current_app.config['MONGO']
    data = mongo.db.request_queue.find({"status": "completed"}).sort("payload.last_updated_at", -1)
    get_page_f = lambda e: e['payload']['page']
    data = utils.filter_by_pages(data, get_page_f, request)
    available_page_names = mongo.db.request_queue.distinct("payload.page.page_name")
    return render_template(
        'completed_requests.html',
        data=data,
        page_names=available_page_names,
        query_params=request.args,        
    )

@main_bp.route('/failed')
def failed_requests():
    mongo = current_app.config['MONGO']
    data = mongo.db.request_queue.find({"status": "failed"}).sort("payload.last_updated_at", -1)
    get_page_f = lambda e: e['payload']['page']
    data = utils.filter_by_pages(data, get_page_f, request)
    available_page_names = mongo.db.request_queue.distinct("payload.page.page_name")
    return render_template(
        'failed_requests.html',
        data=data,
        page_names=available_page_names,
        query_params=request.args,        
    )

@main_bp.route('/archived')
def archived_pages():
    mongo = current_app.config['MONGO']
    data = mongo.db.archived_pages.find().sort("payload.archived_at", -1)
    get_page_f = lambda e: e
    data = utils.filter_by_pages(data, get_page_f, request)
    available_page_names = mongo.db.request_queue.distinct("payload.page.page_name")
    return render_template(
        'archived_pages.html',
        data=data,
        page_names=available_page_names,
        query_params=request.args,        
    )

@main_bp.route('/data')
def data_items():
    mongo = current_app.config['MONGO']
    data = mongo.db.item_store.find().sort("parsed_at", -1)
    get_page_f = lambda e: e['page']
    get_item_f = lambda e: e['item']
    data = utils.filter_by_pages(data, get_page_f, request)
    data = utils.filter_by_items(data, get_item_f, request)
    available_item_types = mongo.db.item_store.distinct("item._type")
    available_page_names = mongo.db.item_store.distinct("page.page_name")
    return render_template(
        'crawled_data.html',
        data=data,
        item_types=available_item_types,
        page_names=available_page_names,
        query_params=request.args,        
    )
