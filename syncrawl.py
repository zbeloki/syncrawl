import requests
from lxml import etree

import hashlib
import bisect
from collections import deque
import abc
from abc import abstractmethod
import json
import os
from datetime import datetime
import argparse
import time
import pdb

# Item: id_ sortzean, kontuan izan id bereko item berri batek aurrekoa ordezkatzen duela.
# Noizean behin (egunero?): datalog kopiatu, kargatu, reduzitu (item obsoletoak kendu), move
# Interfaze bat eskeini Item objektuak bakarrik kargatuko dituena, esportazioak egiteko.
# Datetime-ekin, edozein uneko egoerara itzuli daiteke erraz

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
    def __init__(self, cache_path):
        self._cache = CacheManager(cache_path)

    def download(self, url):
        content = self._cache.retrieve_cached(url)
        if content is None:
            time.sleep(1)
            html = requests.get(url)
            content = html.content.decode()
            self._cache.store_cache(url, content)
        parser = etree.HTMLParser()
        root = etree.fromstring(content, parser)
        return root

    
class JSONSerializable(abc.ABC):
    @abstractmethod
    def to_json(self):
        pass

    @classmethod
    @abstractmethod
    def from_json(cls, obj):
        pass

    
class Item(JSONSerializable):
    def __init__(self, id_, type_, attribs={}):
        self._id = id_
        self._type = type_
        self._attribs = { k:v for k, v in attribs.items() }

    @property
    def id(self):
        return self._id

    @property
    def type(self):
        return self._type
        
    def __str__(self):
        return "[" + self._id + "]" + f"<{self._type}>" + '__'.join([ f"{k}::{self._attribs[k]}" for k in sorted(self._attribs.keys()) ])
    
    def __hash__(self):
        return hash(self._id)
    
    def __eq__(self, other):
        return hash(self) == hash(other)
    
    def __ne__(self, other):
        return not(self == other)

    def to_json(self):
        obj = {
            "_id": self._id,
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
        return '_'.join([ f"{k}:{self._values[k]}" for k in sorted(self._values.keys()) ])
    
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
    _registry = {}
    
    def __init__(self, key):
        self._key = key

    @classmethod
    def register_page(cls, page_name, page_cls):
        cls._registry[page_name] = page_cls

    @property
    def key(self):
        return self._key

    @property
    @abstractmethod
    def name(self):
        pass
    
    @abstractmethod
    def url(self):
        pass

    @abstractmethod
    def next_update(self, last_update, metadata):
        pass

    @abstractmethod
    def parse(self, html, metadata):
        pass

    def __str__(self):
        return "[" + self.name + "]" + str(self._key)
    
    def __hash__(self):
        return hash(str(self))
    
    def __eq__(self, other):
        return str(self) == str(other)
    
    def __ne__(self, other):
        return not(self == other)

    def to_json(self):
        return {
            "name": self.name,
            "key": self._key.to_json(),
        }

    @classmethod
    def from_json(cls, obj):
        if obj['name'] not in cls._registry:
            raise ValueError(f"Unknown page: {obj['name']}")
        page_cls = cls._registry[obj['name']]
        key = Key.from_json(obj['key'])
        return page_cls(key)

    
class PageRequest(JSONSerializable):
    def __init__(self, page, last_timestamp, next_timestamp, metadata={}):
        if next_timestamp is None or (last_timestamp is not None and next_timestamp <= last_timestamp):
            raise ValueError("next_timestamp must contain a value greater than last_timestamp")
        self._page = page
        self._last_timestamp = last_timestamp
        self._next_timestamp = next_timestamp
        self._metadata = metadata

    @property
    def page(self):
        return self._page

    @property
    def last_timestamp(self):
        return self._last_timestamp

    @property
    def next_timestamp(self):
        return self._next_timestamp

    @property
    def metadata(self):
        return self._metadata

    def ready(self):
        return time.time() >= self.next_timestamp

    def __str__(self):
        return f"{str(self.page)}__next:{self.next_timestamp}"

    def __hash__(self):
        return hash(self.page)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not(self==other)

    def to_json(self):
        return {
            "page": self.page.to_json(),
            "last_timestamp": self.last_timestamp,
            "next_timestamp": self.next_timestamp,
            "metadata": "NOT_IMPLEMENTED",
        }

    @classmethod
    def from_json(cls, obj):
        page = Page.from_json(obj["page"])
        metadata = None
        last_timestamp = obj["last_timestamp"]
        next_timestamp = obj["next_timestamp"]
        return cls(page, last_timestamp, next_timestamp, metadata)


class RequestQueue:
    def __init__(self):
        self._request_queue = deque()
        self._queued_pages = set()

    def add_request(self, request):
        if request.page not in self._queued_pages:
            bisect.insort(self._request_queue, request, key=lambda req: req.next_timestamp)
            self._queued_pages.add(request.page)
            return True
        return False

    def remove_request(self, request):
        idx = self._request_queue.index(request)
        del self._request_queue[idx]
        self._queued_pages.remove(request.page)

    def get_next_request(self):
        request = None
        if len(self._request_queue) == 0:
            return None
        while request is None:
            next_request = self._request_queue[0]
            if next_request.ready():
                request = self._request_queue[0]
            else:
                time.sleep(1)
        return request

    
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
        self.metadata = {}

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
    
    def __init__(self, datalog_fpath, cache_path):
        self._downloader = HTTPDownloader(cache_path)
        self._datalog = Datalog(datalog_fpath)
        self._request_queue = RequestQueue()
        self._page_items = {}
        self._forgotten_pages = set()
        
    def _load_state(self):
        for msg in self._datalog.read():
            if msg['event'] == "ADD_ITEM": # item, page
                self._add_item(msg['item'], msg['page'], log=False)
            elif msg['event'] == "DEL_ITEM": # item, page
                self._delete_item(msg['item_id'], msg['page'], log=False)
            elif msg['event'] == "ADD_REQUEST": # request
                self._add_request(msg['request'], log=False)
            elif msg['event'] == "END_REQUEST": # request
                self._end_request(msg["request"], log=False)
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

    def _add_request(self, request, log=True):
        if request.page not in self._forgotten_pages:
            added = self._request_queue.add_request(request)
            if log and added:
                self._datalog.write_add_request(request)

    def _end_request(self, request, log=True):
        if log:
            self._datalog.write_end_request(request)
        self._request_queue.remove_request(request)
        
    def _forget_page(self, page, log=True):
        if log:
            self._datalog.write_forget_page(page)
        self._forgotten_pages.add(page)

    def _produce_items(self, items, page):
        item_ids = set([ item._id for item in items ])
        if page not in self._page_items:
            self._page_items[page] = set()
        for prev_item_id in set(self._page_items[page]):
            if prev_item_id not in item_ids:
                self._delete_item(prev_item_id, page)
        for item in items:
            self._add_item(item, page)

    def process_request(self, request):
        html = self._downloader.download(request.page.url())
        output = request.page.parse(html, request.metadata)
        last_timestamp = request.next_timestamp
        if len(output._items) > 0:
            self._produce_items(output._items, request.page)
        for page in output._pages:
            new_request = PageRequest(page, last_timestamp, time.time(), output.metadata)
            self._add_request(new_request)

        self._end_request(request)

        next_timestamp = request.page.next_update(last_timestamp, output.metadata)
        if next_timestamp is not None:
            new_request = PageRequest(request.page, last_timestamp, next_timestamp, request.metadata)
            self._add_request(new_request)
        else:
            self._forget_page(request.page)
    
    def sync(self):
        self._load_state()
        
        for page in self._root_pages:
            self._add_request(PageRequest(page, None, time.time(), {}))
        
        while True:
            request = self._request_queue.get_next_request()
            if request is None:
                return
            self.process_request(request)


def root_page(keys):
    def wrapper(page_cls):
        for key in keys:
            Crawler._root_pages.append(page_cls(Key(**key)))
        return page_cls
    return wrapper

def register_page(page_cls):
    Page.register_page(page_cls.name, page_cls)
    return page_cls