from ..syncrawl import (
    Utils,
    Key,
    Item,
    Page,
    ParsingOutput,
    PageRequest,
    RequestQueue,
    Datalog,
    Crawler,
    root_page,
)

from tempfile import NamedTemporaryFile, TemporaryDirectory
from collections import deque
from datetime import datetime
import pytest
import time
import pdb

# Utils

def test_utils_are_all_scalar():
    assert Utils.are_all_scalar({"a": 1, "b": "c", "c": 0.12, "c": False}) is True
    assert Utils.are_all_scalar({1: "a"}) is False
    assert Utils.are_all_scalar({"a": ["A", "B"]}) is False
    assert Utils.are_all_scalar({"a": []}) is False
    assert Utils.are_all_scalar({"a": set()}) is False
    assert Utils.are_all_scalar({"a": {}}) is False
    

# Item

def test_item_basic():
    i1 = Item("car_1_a", "car", {"wheels": 4, "windows": 3})
    i2 = Item("car_1_a", "motorbike", {"wheels": 2, "windows": 4})
    i3 = Item("car_3_a", "car", {"wheels": 4, "windows": 3})
    assert i1.id == "car_1_a"
    assert i1.type == "car"
    assert str(i1) == f"[car_1_a]<car>wheels::4__windows::3"
    assert hash(i1) == hash("car_1_a")
    assert len(set([i1, i2])) == 1
    assert len(set([i1, i2, i3])) == 2
    assert i1 == i2
    assert i1 != i3

def test_item_serializable():
    i = Item("car_1_a", "car", {"wheels": 4, "windows": 3})
    i_json = i.to_json()
    i_copy = Item.from_json(i_json)
    assert i == i_copy


# Key

def test_key_basic():
    k = Key(id=5, name="a")
    assert k.id == 5
    assert k.name == "a"
    with pytest.raises(AttributeError):
        k.tag
    with pytest.raises(ValueError):
        Key()
    assert str(k) == "id:5_name:a"

def test_key_scalar_only():
    with pytest.raises(TypeError):
        k = Key(id=1, name=["a"])
    with pytest.raises(TypeError):
        k = Key(id={"name": "a", "lastname": "b"})
    with pytest.raises(TypeError):
        k = Key({"name": "a", "lastname": "b"})

def test_key_cmp_hash():
    k1 = Key(id=2)
    k2 = Key(id=2)
    k3 = Key(id=2, name=2)
    assert k1 == k2
    assert not k1 != k2
    assert k1 != k3
    assert not k1 == k3
    s = set([k1, k2, k3])
    assert len(s) == 2
    d = {k1:"1", k2:"2", k3:"3"}
    assert len(d.keys()) == 2

def test_key_serializable():
    k1 = Key(id=1, name="abc")
    json_k = k1.to_json()
    k2 = Key.from_json(json_k)
    assert k1 == k2


# Page
    
class A(Page):
    name="a"
    def url(self):
        return f"http://test.com/{self.key.name}"
    def next_update(self, last_update, metadata):
        return last_update + 1
    def parse(self, html, metadata):
        out = ParsingOutput()
        out.add_page(A(Key(id=4, name="abc")))
        return out

class B(Page):
    def url(self):
        return f"http://test.com/{self.key.name}"
    def next_update(self, last_update, metadata):
        return last_update + 1
    def parse(self, html, metadata):
        out = ParsingOutput()
        out.add_page(Page(k1))
        return out

class C(Page):
    name = "c"
    def next_update(self, last_update, metadata):
        return last_update + 1
    def parse(self, html, metadata):
        out = ParsingOutput()
        out.add_page(Page(k1))
        return out
    
def test_page_basic():
    k1 = Key(id=5, name="abc")
    k2 = Key(id=5, name="abc")
    p = A(k1)
    assert p.key == k2

def test_page_subclass():
    k1 = Key(id=5, name="abc")
    k2 = Key(id=6, name="def")
    p1 = A(k1)
    p2 = A(k2)
    p3 = A(k2)
    assert p1.name == "a"
    assert p1.url() == "http://test.com/abc"
    assert p1.next_update(1, {}) == 2
    assert len(p1.parse("html", {})._pages) == 1
    Page.register_page(p1.name, A)
    assert Page._registry == {"a": A}
    assert str(p1) == f"[a]{str(p1.key)}"
    assert hash(p1) == hash(f"[a]{str(p1.key)}")
    assert hash(p2) != hash(p1)
    assert hash(p2) == hash(p3)
    with pytest.raises(TypeError):
        pb = B(k1)
    with pytest.raises(TypeError):
        pc = C(k1)
    
def test_page_serializable():
    p = A(Key(id=5, name="abc"))
    p_json = p.to_json()
    p_copy = Page.from_json(p_json)
    assert type(p_copy) == A
    assert p == p_copy
    

# PageRequest

def test_page_request_basic():
    p = A(Key(id=1))
    with pytest.raises(ValueError):
        pr = PageRequest(p, 1, None)
    with pytest.raises(ValueError):
        pr = PageRequest(p, 1, 1)
    pr = PageRequest(p, None, 1.2)
    assert pr.page == p
    assert pr.last_timestamp is None
    assert pr.next_timestamp == 1.2
    assert pr.metadata == {}
    f_time = time.time
    time.time = lambda: 1.19
    assert pr.ready() is False
    time.time = lambda: 1.20
    assert pr.ready() is True
    time.time = lambda: 1.21
    assert pr.ready() is True
    time.time = f_time
    assert str(pr) == f"{str(p)}__next:1.2"
    assert hash(pr) == hash(p)
    pr2 = PageRequest(A(Key(id=2)), 1, 1.2)
    assert hash(pr) != hash(pr2)
    assert pr == PageRequest(p, 1, 1.2)
    assert pr == PageRequest(p, 1.5, 3.3)
    assert pr != pr2

def test_page_request_serializable():
    pr = PageRequest(A(Key(id=1)), 1, 1.2)
    pr_json = pr.to_json()
    pr_copy = PageRequest.from_json(pr_json)
    assert pr == pr_copy


# RequestQueue

def test_request_queue():
    q = RequestQueue()
    p1 = A(Key(id=1))
    p2 = A(Key(id=2))
    p3 = A(Key(id=3))
    pr1a = PageRequest(p1, 4, 7)
    pr1b = PageRequest(p1, 2, 4)
    pr2 = PageRequest(p2, 5, 6)
    pr3a = PageRequest(p3, None, 8)
    pr3b = PageRequest(p3, None, 2)
    q.add_request(pr1a)
    q.add_request(pr1b)
    q.add_request(pr2)
    q.add_request(pr3a)
    q.add_request(pr3b)
    assert q._request_queue == deque([pr2, pr1a, pr3a])
    assert q._queued_pages == set([pr1a, pr2, pr3a])
    assert q._queued_pages == set([pr1b, pr2, pr3b])
    assert q._request_queue[0].next_timestamp == 6
    assert q._request_queue[1].next_timestamp == 7
    assert q._request_queue[2].next_timestamp == 8
    q.remove_request(pr1b)
    assert q._request_queue == deque([pr2, pr3a])
    assert q._queued_pages == set([pr2, pr3a])
    pr1c = PageRequest(p1, None, 6.5)
    q.add_request(pr1c)
    assert q._request_queue == deque([pr2, pr1c, pr3a])
    assert q._request_queue != deque([pr1c, pr2, pr3a])
    assert q._queued_pages == set([pr1c, pr2, pr3a])
    assert q._queued_pages != set([pr2, pr3a])
    f_time = time.time
    time.time = lambda: 10
    assert q.get_next_request() == pr2
    assert q._request_queue == deque([pr2, pr1c, pr3a])
    assert q._queued_pages == set([pr1c, pr2, pr3a])
    q.remove_request(pr2)
    assert q._request_queue == deque([pr1c, pr3a])
    assert q._queued_pages == set([pr1c, pr3a])
    assert q.get_next_request() == pr1c
    assert q._request_queue == deque([pr1c, pr3a])
    assert q._queued_pages == set([pr1c, pr3a])
    q.remove_request(pr1c)
    assert q._request_queue == deque([pr3a])
    assert q._queued_pages == set([pr3a])
    q.remove_request(pr3a)
    assert q._request_queue == deque()
    assert q._queued_pages == set()
    assert q.get_next_request() is None
    
    
# Datalog

def test_datalog():
    p = A(Key(id=1, name="abc"))
    pr = PageRequest(p, 1, 2)
    i = Item("car_1", "car", {"wheels": 4, "windows": 2})
    with NamedTemporaryFile(mode='w') as f:
        log = Datalog(f.name)
        log.write_add_item(i, p)
        log.write_delete_item("car_1", p)
        log.write_add_request(pr)
        log.write_end_request(pr)
        log.write_forget_page(p)
        events = log.read()
    for event in events:
        assert type(event['datetime']) == datetime
        del event['datetime']
    assert events[0] == {
        "event": "ADD_ITEM",
        "page": p,
        "item": i,
    }
    assert events[1] == {
        "event": "DEL_ITEM",
        "page": p,
        "item_id": "car_1",
    }
    assert events[2] == {
        "event": "ADD_REQUEST",
        "request": pr,
    }
    assert events[3] == {
        "event": "END_REQUEST",
        "request": pr,
    }
    assert events[4] == {
        "event": "FORGET_PAGE",
        "page": p,
    }
    

# Crawler

def test_crawler_basic():
    i1 = Item("car_1", "car", {"windows": 4})
    i2 = Item("car_2", "car", {"windows": 3})
    i3 = Item("car_3", "car", {"windows": 2})
    p1 = A(Key(id=1, name="abc"))
    p2 = A(Key(id=2, name="def"))
    p3 = A(Key(id=3, name="ghi"))
    p4 = A(Key(id=4, name="jkl"))
    pr1 = PageRequest(p1, 1, 2)
    pr2a = PageRequest(p2, 3, 4)
    pr2b = PageRequest(p2, 6, 7)
    pr3 = PageRequest(p3, 9, 10)
    pr4 = PageRequest(p4, 10, 11)
    with NamedTemporaryFile(mode='w') as f_log, \
         TemporaryDirectory() as tmp:
        crawler = Crawler(f_log.name, tmp)
        crawler._add_item(i1, p1)
        assert p1 in crawler._page_items
        assert crawler._page_items[p1] == set(["car_1"])
        assert len(crawler._page_items) == 1
        crawler._delete_item("car_1", p1)
        assert len(crawler._page_items) == 0
        crawler._add_request(pr1)
        assert len(crawler._forgotten_pages) == 0
        assert len(crawler._request_queue._request_queue) == 1
        crawler._end_request(pr1)
        assert len(crawler._forgotten_pages) == 0
        assert len(crawler._request_queue._request_queue) == 0
        crawler._forget_page(p1)
        assert crawler._forgotten_pages == set([p1])
        crawler._add_request(pr1)
        assert len(crawler._request_queue._request_queue) == 0
        crawler._add_request(pr2a)
        assert len(crawler._request_queue._request_queue) == 1
        crawler._add_request(pr2b)
        assert len(crawler._request_queue._request_queue) == 1
        crawler._add_request(pr3)
        assert len(crawler._request_queue._request_queue) == 2
        crawler._add_item(i1, p2)
        crawler._add_item(i2, p2)
        assert len(crawler._page_items) == 1
        assert len(crawler._page_items[p2]) == 2
        crawler._produce_items([i1, i3], p2)
        assert len(crawler._page_items) == 1
        assert len(crawler._page_items[p2]) == 2
        with open(f_log.name, 'r') as f_log_r:
            n_events = len(f_log_r.readlines())
        # load 2. crawler
        crawler2 = Crawler(f_log.name, tmp)
        crawler2._load_state()
        assert crawler2._forgotten_pages == set([p1])
        assert len(crawler2._request_queue._request_queue) == 2
        assert len(crawler2._page_items) == 1
        assert len(crawler2._page_items[p2]) == 2
        assert crawler2._page_items[p2] == set(["car_1", "car_3"])
        with open(f_log.name, 'r') as f_log_r:
            assert len(f_log_r.readlines()) == n_events
        # process request
        crawler._downloader.download = lambda url: "<html/>"
        out1 = ParsingOutput()
        out1.add_item(i2)
        out1.add_item(i3)
        out1.add_page(p4)
        p3.parse = lambda html, meta: out1
        time.time = lambda: 15
        assert p3 not in crawler._page_items
        crawler.process_request(pr3)
        assert p3 in crawler._page_items
        assert crawler._page_items[p3] == set([i2, i3])
        assert len(crawler._request_queue._request_queue) == 3
        assert crawler._request_queue._request_queue[-1].page == p4

def test_crawler_root_pages():
    assert Crawler._root_pages == []
    @root_page([
        {"id":1, "name":"abc"},
        {"id":2, "name":"def"},
    ])
    class F(Page):
        name="f"
        def url(self):
            return f"http://test.com/{self.key.name}"
        def next_update(self, last_update, metadata):
            return last_update + 1
        def parse(self, html, metadata):
            out = ParsingOutput()
            out.add_page(A(Key(id=4, name="abc")))
            return out
    assert Crawler._root_pages == [
        F(Key(id=1, name="abc")),
        F(Key(id=2, name="def")),
    ]
            
