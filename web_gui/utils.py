from flask import current_app

def filter_by_pages(data, get_page_f, request):
    page_name = request.args.get('page_name', '')
    page_key_pattern = request.args.get('page_key', '')
    if page_name.strip() != "":
        data = filter_by_page_name(data, get_page_f, page_name)
    if page_key_pattern.strip() != "":
        data = filter_by_search_key(data, get_page_f, page_key_pattern)
    return data

def filter_by_page_name(data, get_page_f, page_name):
    return [ e for e in data if get_page_f(e)['page_name'] == page_name ]

def filter_by_search_key(data, get_page_f, pattern):
    format_key = current_app.jinja_env.filters['format_key']
    result = []
    for e in data:
        e_str = format_key(get_page_f(e)['key']).lower()
        if all([ token in e_str for token in pattern.lower().split() ]):
            result.append(e)
    return result

def filter_by_items(data, get_item_f, request):
    item_type = request.args.get('item_type', '')
    item_pattern = request.args.get('item_pattern', '')
    if item_type.strip() != "":
        data = filter_by_item_type(data, get_item_f, item_type)
    if item_pattern.strip() != "":
        data = filter_by_search_data(data, get_item_f, item_pattern)
    return data    

def filter_by_item_type(data, get_item_f, item_type):
    return [ e for e in data if get_item_f(e)['_type'] == item_type ]

def filter_by_search_data(data, get_item_f, pattern):
    format_item = current_app.jinja_env.filters['format_item']
    result = []
    for e in data:
        e_str = format_item(get_item_f(e)).lower()
        if all([ token in e_str for token in pattern.lower().split() ]):
            result.append(e)
    return result

