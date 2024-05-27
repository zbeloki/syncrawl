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

# Item: id_ sortzean, kontuan izan id bereko item berri batek aurrekoa ordezkatzen duela.
# Noizean behin (egunero?): datalog kopiatu, kargatu, reduzitu (item obsoletoak kendu), move
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
    
    def __hash__(self):
        return hash(self._id)
    
    def __eq__(self, other):
        return hash(self) == hash(other)
    
    def __ne__(self, other):
        return not(self == other)

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
    
    def __hash__(self):
        return hash(str(self))
    
    def __eq__(self, other):
        return str(self) == str(other)
    
    def __ne__(self, other):
        return not(self == other)

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
    def next_update(self, last_update):
        pass

    @abstractmethod
    def parse(self, html):
        pass

    def __str__(self):
        key_str = str(self._key) if self._key is not None else ""
        return "<" + self.page_name + ">" + key_str
    
    def __hash__(self):
        return hash(str(self))
    
    def __eq__(self, other):
        return str(self) == str(other)
    
    def __ne__(self, other):
        return not(self == other)

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
    def __init__(self, page, last_timestamp, next_timestamp, id_=None):
        if next_timestamp is None or (last_timestamp is not None and next_timestamp <= last_timestamp):
            raise ValueError("next_timestamp must contain a value greater than last_timestamp")
        self._page = page
        self._last_timestamp = last_timestamp
        self._next_timestamp = next_timestamp
        self._id = id_

    @property
    def id(self):
        return self._id

    @property
    def page(self):
        return self._page

    @property
    def last_timestamp(self):
        return self._last_timestamp

    @property
    def next_timestamp(self):
        return self._next_timestamp

    def __str__(self):
        date = datetime.fromtimestamp(self.next_timestamp)
        return f"{str(self.page)}(Update:{date.strftime('%Y-%m-%d_%H:%M:%S')})"

    def to_json(self):
        return {
            "page": self.page.to_json(),
            "last_timestamp": self.last_timestamp,
            "next_timestamp": self.next_timestamp,
        }

    @classmethod
    def from_json(cls, obj, id_):
        page = Page.from_json(obj["page"])
        last_timestamp = obj["last_timestamp"]
        next_timestamp = obj["next_timestamp"]
        return cls(page, last_timestamp, next_timestamp, id_=id_)


class ItemStore:
    def __init__(self, db):
        self._is = db.item_store
        self._create_indices()

    def _create_indices(self):
        # @: create all indices
        pass

    def add_items(self, items, page):
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
        self._rq.create_index("payload.next_timestamp")
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
        threshold_ts = datetime.now() - timedelta(seconds=max_processing_time)
        self._rq.update_many(
            {
                "status": "processing",
                "processing_started_at": {"$lte": threshold_ts }
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
                    "payload.next_timestamp": {"$lte": datetime.now().timestamp()},
                },
                {
                    "$set": {
                        "status": "processing",
                        "status_updated_at": datetime.now(),
                    },
                },
                sort=[("payload.next_timestamp", 1)]
            )
            if request_json is None:
                # @: sleep time configurable
                time.sleep(1)                
            else:
                return PageRequest.from_json(request_json["payload"], id_=request_json["_id"])
    
class Datalog:
    datetime_format = "%m-%d-%Y %H:%M:%S.%f"
    
    def __init__(self, fpath):
        self._fpath = fpath

    def _write(self, msg):
        msg["datetime"] = datetime.now().strftime(self.datetime_format)
        with open(self._fpath, 'a') as f:
            print(json.dumps(msg), file=f)

    def read(self):
        msgs = []
        if os.path.isfile(self._fpath):
            with open(self._fpath, 'r') as f:
                for ln in f:
                    msg = json.loads(ln)
                    for key, value in msg.items():
                        if key == "page":
                            msg[key] = Page.from_json(value)
                        elif key == "item":
                            msg[key] = Item.from_json(value)
                        elif key == "request":
                            msg[key] = PageRequest.from_json(value)
                    if "datetime" in msg:
                        msg["datetime"] = datetime.strptime(msg["datetime"], self.datetime_format)
                    msgs.append(msg)
        return msgs

    def write_add_item(self, item, page):
        self._write({
            "event": "ADD_ITEM",
            "page": page.to_json(),
            "item": item.to_json(),
        })

    def write_delete_item(self, item_id, page):
        self._write({
            "event": "DEL_ITEM",
            "page": page.to_json(),
            "item_id": item_id,
        })

    def write_add_request(self, request):
        self._write({
            "event": "ADD_REQUEST",
            "request": request.to_json(),
        })

    def write_end_request(self, request):
        self._write({
            "event": "END_REQUEST",
            "request": request.to_json(),
        })

    def write_forget_page(self, page):
        self._write({
            "event": "FORGET_PAGE",
            "page": page.to_json(),
        })


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

    
# class DatabaseManager:
#     def __init__(self, db_name):
#         # @: MongoDB config
#         self._client = MongoClient()
#         self._db = self._client[db_name]
#         self.request_queue = self._db.request_queue
#         # @: change next_timestamp -> next_datetime
#         self.request_queue.create_index("next_timestamp")
#         self.request_queue.create_index(["page.page_name", "page.key"])

#     def add_request(self, request):
#         if self.request_queue.count_documents({
#                 "page.page_name": request.page.to_json()["page_name"],
#                 "page.key": request.page.to_json()["key"],
#         }, limit=1) == 0:
#             self.request_queue.insert_one(request.to_json())
#             return True
#         return False

#     def remove_request(self, request):
#         self.request_queue.delete_one({
#             "page.page_name": request.page.to_json()["page_name"],
#             "page.key": request.page.to_json()["key"],
#         })

#     def get_next_request(self):
#         while True:
#             cursor = self.request_queue.find().sort({"next_timestamp": 1}).limit(1)
#             request_json = next(cursor, None)
#             if request_json is None:
#                 return None
#             if time.time() >= request_json["next_timestamp"]:
#                 return PageRequest.from_json(request_json)
#             else:
#                 time.sleep(1)

class Crawler:
    _root_pages = []
    
    def __init__(self, db_name, datalog_fpath, cache_path=None, request_delay=0):
        self._client = MongoClient()
        self._db = self._client[db_name]
        self._downloader = HTTPDownloader(cache_path, request_delay)
        self._datalog = Datalog(datalog_fpath)
        self._request_queue = RequestQueue(self._db)
        self._item_store = ItemStore(self._db)
        self._page_items = {}
        #self._db = DatabaseManager(db_name)
        
    def _load_state(self):
        for msg in self._datalog.read():
            if msg['event'] == "ADD_ITEM": # item, page
                self._add_item(msg['item'], msg['page'], log=False)
            elif msg['event'] == "DEL_ITEM": # item, page
                self._delete_item(msg['item_id'], msg['page'], log=False)
            elif msg['event'] == "FORGET_PAGE": # page
                self._forget_page(msg["page"], log=False)

    def _add_item(self, item, page, log=True):
        if log:
            self._datalog.write_add_item(item, page)
        if page not in self._page_items:
            self._page_items[page] = set()
        self._page_items[page].add(item._id)

    def _delete_item(self, item_id, page, log=True):
        if log:
            self._datalog.write_delete_item(item_id, page)
        if page in self._page_items and item_id in self._page_items[page]:
            self._page_items[page].remove(item_id)
            if len(self._page_items[page]) == 0:
                del self._page_items[page]

    def _add_request(self, request, force=False, log=True):
        if force or not self._request_queue.is_page_archived(request.page):
            added = self._request_queue.add_request(request, force)
            if added:
                logging.info(f"New request {request} added")

    def _end_request(self, request, log=True):
        self._request_queue.end_request(request)

    def _fail_request(self, request, msg, traceback_msg, force=False):
        self._request_queue.fail_request(request, msg, traceback_msg, force)
        
    def _forget_page(self, page, log=True):
        self._request_queue.archive_page(page)

    def _produce_items(self, items, page):
        self._item_store.add_items(items, page)
        # item_ids = set([ item._id for item in items ])
        # if page not in self._page_items:
        #     self._page_items[page] = set()
        # for prev_item_id in set(self._page_items[page]):
        #     if prev_item_id not in item_ids:
        #         logging.info(f"Item {prev_item_id} deleted from page {page}")
        #         self._delete_item(prev_item_id, page)
        # for item in items:
        #     self._add_item(item, page)
        for item in items:
            logging.info(f"Item {item} created from page {page}")

    def process_request(self, request):
        logging.info(f"Processing request {request}")
        try:
            html = self._downloader.download(request.page.url())
            output = request.page.parse(html)
            last_timestamp = request.next_timestamp
            if len(output._items) > 0:
                self._produce_items(output._items, request.page)
            for page in output._pages:
                new_request = PageRequest(page, last_timestamp, time.time())
                self._add_request(new_request)

            last_dt = datetime.fromtimestamp(last_timestamp)
            next_dt = request.page.next_update(last_dt)
            if next_dt is not None:
                next_dt = next_dt.timestamp()
                new_request = PageRequest(request.page, last_timestamp, next_dt)
                self._add_request(new_request, force=True)
            else:
                self._request_queue.archive_page(request.page)
                logging.info(f"Page {request.page} added to archived list")

            self._end_request(request)
        except ParsingError as e:
            self._fail_request(request, e.message, traceback.format_exc(), force=True)
        except Exception as e:
            self._fail_request(request, e.message, traceback.format_exc())

        # last_dt = datetime.fromtimestamp(last_timestamp)
        # next_dt = request.page.next_update(last_dt)
        # if next_dt is not None:
        #     next_dt = next_dt.timestamp()

        # # @: what if the process stops here? request is closed but the next update is not registered yet
        # if next_dt is not None:
        #     new_request = PageRequest(request.page, last_timestamp, next_dt)
        #     self._add_request(new_request, force=True)
        # else:
        #     self._forget_page(request.page)
        #     logging.info(f"Page {request.page} added to forget list")
    
    def sync(self):
        self._load_state()
        
        for page in self._root_pages:
            self._add_request(PageRequest(page, None, time.time()))
        
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
