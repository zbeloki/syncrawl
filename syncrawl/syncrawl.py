import requests
from lxml import etree
from pymongo import MongoClient

from types import NoneType
import hashlib
import bisect
from collections import deque
import abc
from abc import abstractmethod
import json
import os
from datetime import datetime, timedelta
from inspect import isclass
import traceback
import logging
import argparse
import time
import sys
import pdb

# Interfaze bat eskeini Item objektuak bakarrik kargatuko dituena, esportazioak egiteko.
# Datetime-ekin, edozein uneko egoerara itzuli daiteke erraz

class ParsingError(Exception):
    def __init__(self, msg):
        self.message = msg

class Utils:
    @classmethod
    def are_all_scalar(cls, dictionary):
        scalar_types = (int, float, str, bool)  # Add more scalar types as needed
        for value in dictionary.values():
            if not isinstance(value, scalar_types):
                return False
        for key in dictionary.keys():
            if not isinstance(key, str):
                return False
        return True
    
class CacheManager:
    def __init__(self, path):
        os.makedirs(path, exist_ok=True)
        self._path = path
        self._cached = set(os.listdir(path))

    def retrieve_cached(self, url):
        md5 = hashlib.md5(url.encode()).hexdigest()
        if md5 not in self._cached:
            return None
        fpath = os.path.join(self._path, md5)
        with open(fpath, 'r') as f:
            content = f.read()
        return content

    def store_cache(self, url, content):
        md5 = hashlib.md5(url.encode()).hexdigest()
        fpath = os.path.join(self._path, md5)
        with open(fpath, 'w') as f:
            f.write(content)
        self._cached.add(md5)

        
class HTTPDownloader:
    def __init__(self, cache_path, request_delay):
        self._cache = None
        if cache_path is not None:
            self._cache = CacheManager(cache_path)
        self._request_delay = request_delay
        self._last_request = None

    def download(self, url):
        content = None
        if self._cache is not None:
            content = self._cache.retrieve_cached(url)
        if content is None:
            self._wait()
            logging.info(f"Downloading: {url}")
            html = requests.get(url)
            self._last_request = time.time()
            content = html.content.decode()
            if self._cache is not None:
                self._cache.store_cache(url, content)
        else:
            logging.info(f"Retrieving from cache: {url}")
        parser = etree.HTMLParser()
        root = etree.fromstring(content, parser)
        return root

    def _wait(self):
        while self._last_request is not None and time.time() < self._last_request + self._request_delay:
            time.sleep(0.1)
            
    
class JSONSerializable(abc.ABC):
    @abstractmethod
    def to_json(self):
        pass

    @classmethod
    @abstractmethod
    def from_json(cls, obj):
        pass

    
class Item(JSONSerializable):
    accepted_types = [int, float, str, bool, list, dict, tuple, NoneType]
    
    def __init__(self, id_, type_, attribs={}):
        self._id = id_
        self._type = type_
        self._attribs = {}
        for k, v in attribs.items():
            if type(k) != str:
                raise TypeError("Item attrib keys must be of type str")
            if type(v) not in self.accepted_types:
                types = [ str(t) for t in self.accepted_types ]
                raise TypeError(f"Item attrib values must be one of: {', '.join(types)}")
            self._attribs[k] = v

    @property
    def id(self):
        return self._id

    @property
    def type(self):
        return self._type
        
    def __str__(self):
        return "[" + self._id + "]" + f"<{self._type}>{{" + ','.join([ f"{k}:{self._attribs[k]}" for k in sorted(self._attribs.keys()) ]) + "}"

    def to_json(self):
        obj = {
            "_type": self._type,
        }
        for key, value in self._attribs.items():
            obj[key] = value
        return obj

    @classmethod
    def from_json(cls, obj):
        attribs = { key: value for key, value in obj.items() if not key.startswith("_") }
        return cls(obj["_id"], obj["_type"], attribs)

    
class Key(JSONSerializable):
    def __init__(self, **kwargs):
        if kwargs == {}:
            raise ValueError("Key must not be empty")
        if not Utils.are_all_scalar(kwargs):
            raise TypeError("All Key parameter values must be scalar")
        self._values = kwargs

    def __getattr__(self, name):
        if name in self._values:
            return self._values[name]
        else:
            raise AttributeError(f"Keys have no attribute '{name}'")

    def __str__(self):
        return "(" + ','.join([ f"{k}={self._values[k]}" for k in sorted(self._values.keys()) ]) + ")"

    def to_json(self):
        return self._values

    @classmethod
    def from_json(cls, obj):
        return cls(**obj)

    
class Page(JSONSerializable):
    accepted_types = [int, float, str, bool, list, tuple, dict, NoneType]
    _registry = {}
    
    def __init__(self, key=None, **kwargs):
        self._key = key
        self._attribs = {}
        for key, value in kwargs.items():
            self[key] = value

    @classmethod
    def register_page(cls, page_name, page_cls):
        cls._registry[page_name] = page_cls

    @property
    def key(self):
        return self._key

    def __setitem__(self, key, value):
        if type(key) != str:
            raise TypeError("Page attribute keys must be of type str")
        if type(value) not in self.accepted_types:
            types = [ str(t) for t in self.accepted_types ]
            raise TypeError(f"Page attribute values must be one of: {', '.join(types)}")
        self._attribs[key] = value

    def __getitem__(self, key):
        if key not in self._attribs.keys():
            if key not in self._key._values:
                raise AttributeError(f"Key {key} not found among Page attributes")
            else:
                return self._key._values[key]
        return self._attribs[key]
            
    @property
    @abstractmethod
    def page_name(self):
        pass
    
    @abstractmethod
    def url(self):
        pass

    @abstractmethod
    def next_update_at(self, last_updated_at):
        pass

    @abstractmethod
    def parse(self, html):
        pass

    def __str__(self):
        key_str = str(self._key) if self._key is not None else ""
        return "<" + self.page_name + ">" + key_str

    def to_json(self):
        return {
            "page_name": self.page_name,
            "key": self.key.to_json() if self.key is not None else None,
            "attributes": self._attribs,
        }

    @classmethod
    def from_json(cls, obj):
        if obj['page_name'] not in cls._registry:
            raise ValueError(f"Unknown page: {obj['page_name']}")
        page_cls = cls._registry[obj['page_name']]
        key = Key.from_json(obj['key']) if obj['key'] is not None else None
        return page_cls(key, **obj['attributes'])

    
class PageRequest(JSONSerializable):
    def __init__(self, page, last_updated_at, next_update_at, id_=None):
        if next_update_at is None or (last_updated_at is not None and next_update_at <= last_updated_at):
            raise ValueError("next_update_at must contain a value greater than last_updated_at")
        self._page = page
        self._last_updated_at = last_updated_at
        self._next_update_at = next_update_at
        self._id = id_

    @property
    def id(self):
        return self._id

    @property
    def page(self):
        return self._page

    @property
    def last_updated_at(self):
        return self._last_updated_at

    @property
    def next_update_at(self):
        return self._next_update_at

    def __str__(self):
        return f"{str(self.page)}(Update:{self._next_update_at.strftime('%Y-%m-%d_%H:%M:%S')})"

    def to_json(self):
        return {
            "page": self.page.to_json(),
            "last_updated_at": self.last_updated_at,
            "next_update_at": self.next_update_at,
        }

    @classmethod
    def from_json(cls, obj, id_):
        page = Page.from_json(obj["page"])
        last_updated_at = obj["last_updated_at"]
        next_update_at = obj["next_update_at"]
        return cls(page, last_updated_at, next_update_at, id_=id_)


class ItemStore:
    def __init__(self, db):
        self._is = db.item_store
        self._create_indices()

    def _create_indices(self):
        # @: create all indices
        pass

    def set_items(self, items, page):
        self._is.delete_many({
            "page.page_name": page.to_json()["page_name"],
            "page.key": page.to_json()["key"],
        })
        item_jsons = [ {
            "item": item.to_json(),
            "page": {
                "page_name": page.to_json()["page_name"],
                "key": page.to_json()["key"],
            },
        } for item in items ]
        self._is.insert_many(item_jsons)

class RequestQueue:
    def __init__(self, db):
        self._rq = db.request_queue
        self._ap = db.archived_pages
        self._create_indices()

    def _create_indices(self):
        # @: create all indices
        self._rq.create_index("payload.next_update_at")
        self._rq.create_index(["payload.page.page_name", "payload.page.key"])
        self._ap.create_index(["payload.page_name", "payload.key"])
        
    def add_request(self, request, force=False):
        if force or self._rq.count_documents({
                "status": {"$in": ["pending", "processing", "failed"]},
                "payload.page.page_name": request.page.to_json()["page_name"],
                "payload.page.key": request.page.to_json()["key"],
        }, limit=1) == 0:
            self._rq.insert_one({
                "payload": request.to_json(),
                "status": "pending",
                "created_at": datetime.now(),
                "status_updated_at": datetime.now(),
                "processing_started_at": None,
                "retries": 0,
            })
            return True
        return False
    
    def end_request(self, request):
        # @: max_retries configurable
        max_retries = 2
        self._rq.update_one(
            {
                "_id": request.id,
                "status": "processing",
                "payload.page.page_name": request.page.to_json()["page_name"],
                "payload.page.key": request.page.to_json()["key"],
            },
            {
                "$set": {
                    "status": "completed",
                    "status_updated_at": datetime.now(),
                    "payload.next_update_at": None,
                },
            },
        )

    def fail_request(self, request, error_msg, traceback_msg, force=False):
        # @: use id to select the request in the DB
        self._rq.update_one(
            {
                "_id": request.id,
                "status": "processing",
                "payload.page.page_name": request.page.to_json()["page_name"],
                "payload.page.key": request.page.to_json()["key"],
            },
            {
                "$set": {
                    "status": ("failed" if force else "pending"),
                    "status_updated_at": datetime.now(),
                    "error_msg": error_msg,
                    "error_traceback": traceback_msg,
                },
                "$inc": {"retries": 1},
            },
        )

    def archive_page(self, page):
        self._ap.insert_one({
            "page_name": page.to_json()["page_name"],
            "key": page.to_json()["key"],
        })

    def is_page_archived(self, page):
        return self._ap.count_documents({
                "payload.page_name": page.to_json()["page_name"],
                "payload.key": page.to_json()["key"],
        }, limit=1) == 1

    def _check_stale_requests(self):
        # @: max_processing_time configurable
        # @: any request started processing before max_processing_time must be forced to fail
        max_processing_time = 10
        threshold_dt = datetime.now() - timedelta(seconds=max_processing_time)
        self._rq.update_many(
            {
                "status": "processing",
                "processing_started_at": {"$lte": threshold_dt }
            },
            {
                "$set": {
                    "status": "pending",
                    "status_updated_at": datetime.now(),
                    "processing_started_at": None,
                },
                "$inc": {
                    "retries": 1,
                },
            },
        )

    def _check_failed_requests(self):
        # @: max_retries configurable
        max_retries = 2
        self._rq.update_many(
            {
                "status": "pending",
                "retries": {"$gte": max_retries+1},
            },
            {
                "$set": {
                    "status": "failed",
                    "status_updated_at": datetime.now(),
                },
            },
        )
        
    def get_next_request(self):
        while True:
            self._check_stale_requests()
            self._check_failed_requests()
            request_json = self._rq.find_one_and_update(
                {
                    "status": "pending",
                    "payload.next_update_at": {"$lte": datetime.now()},
                },
                {
                    "$set": {
                        "status": "processing",
                        "status_updated_at": datetime.now(),
                    },
                },
                sort=[("payload.next_update_at", 1)]
            )
            if request_json is None:
                # @: sleep time configurable
                time.sleep(1)                
            else:
                return PageRequest.from_json(request_json["payload"], id_=request_json["_id"])
    

class ParsingOutput:
    def __init__(self):
        self._items = []
        self._pages = []

    @property
    def items(self):
        return self._items

    @property
    def pages(self):
        return self._pages
    
    def add_item(self, item):
        self._items.append(item)

    def add_page(self, page):
        self._pages.append(page)


class Crawler:
    _root_pages = []
    
    def __init__(self, db_name, datalog_fpath, cache_path=None, request_delay=0):
        self._client = MongoClient()
        self._db = self._client[db_name]
        self._downloader = HTTPDownloader(cache_path, request_delay)
        self._request_queue = RequestQueue(self._db)
        self._item_store = ItemStore(self._db)

    def _add_request(self, request, force=False):
        if force or not self._request_queue.is_page_archived(request.page):
            added = self._request_queue.add_request(request, force)
            if added:
                logging.info(f"New request {request} added")

    def process_request(self, request):
        logging.info(f"Processing request {request}")
        try:
            html = self._downloader.download(request.page.url())
            output = request.page.parse(html)
            last_updated_at = request.next_update_at
            if len(output._items) > 0:
                self._item_store.set_items(output._items, request.page)
                for item in output._items:
                    logging.info(f"Item {item} created from page {request.page}")
            for page in output._pages:
                new_request = PageRequest(page, last_updated_at, datetime.now())
                self._add_request(new_request)

            next_update_at = request.page.next_update_at(last_updated_at)
            if next_update_at is not None:
                new_request = PageRequest(request.page, last_updated_at, next_update_at)
                self._add_request(new_request, force=True)
            else:
                self._request_queue.archive_page(request.page)
                logging.info(f"Page {request.page} added to archived list")

            self._request_queue.end_request(request)
        except ParsingError as e:
            self._request_queue.fail_request(request, e.message, traceback.format_exc(), force=True)
        except Exception as e:
            self._request_queue.fail_request(request, e.message, traceback.format_exc())
    
    def sync(self):        
        for page in self._root_pages:
            self._add_request(PageRequest(page, None, datetime.now()))
        
        while True:
            request = self._request_queue.get_next_request()
            self.process_request(request)


def root_page(keys):
    if isclass(keys):
        # @root_page
        page_cls = keys
        Crawler._root_pages.append(page_cls())
    else:
        # @root_page([key])
        def wrapper(page_cls):
            for key in keys:
                Crawler._root_pages.append(page_cls(Key(**key)))
            return page_cls
        return wrapper

def register_page(page_cls):
    Page.register_page(page_cls.page_name, page_cls)
    return page_cls
