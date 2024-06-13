from flask import current_app

def filter_by_pages(request_objs, request):
    page_name = request.args.get('page_name', '')
    page_key_pattern = request.args.get('page_key', '')
    if page_name.strip() != "":
        request_objs = filter_requests_by_page_name(request_objs, page_name)
    if page_key_pattern.strip() != "":
        request_objs = filter_requests_by_search(request_objs, page_key_pattern)
    return request_objs

def filter_requests_by_page_name(request_objs, page_name):
    return [ req for req in request_objs if req['payload']['page']['page_name'] == page_name ]

def filter_requests_by_search(request_objs, pattern):
    format_key = current_app.jinja_env.filters['format_key']
    result_requests = []
    for request_obj in request_objs:
        request_str = format_key(request_obj['payload']['page']['key']).lower()
        if all([ token in request_str for token in pattern.lower().split() ]):
            result_requests.append(request_obj)
    return result_requests

