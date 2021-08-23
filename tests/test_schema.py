import pytest

from voluptuous.error import MultipleInvalid
from shell_pipeline.schema import schema


def test_schema_ok():
    conf = {"version": 1.2, "output_dir": "/tmp/output", "command": "pwd"}
    schema(conf)
 
    with pytest.raises(MultipleInvalid):
        conf["version"]=1
        schema(conf)
    conf["version"]=1.2

    with pytest.raises(MultipleInvalid):
        conf["new"]=1
        schema(conf)
    conf.pop("new")

