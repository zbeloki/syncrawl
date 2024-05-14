from ..syncrawl import Key

import pytest
import pdb

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
    
