import json
import logging

from taskqueue import logs


def test_json_lines_carry_job_context():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "job started", None, None)
    with logs.log_context(job_id="abc", attempt=2):
        line = json.loads(logs.JsonFormatter().format(record))
    assert line["msg"] == "job started"
    assert line["job_id"] == "abc"
    assert line["attempt"] == 2


def test_context_is_cleared_after_block():
    with logs.log_context(job_id="abc"):
        pass
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "idle", None, None)
    assert "job_id" not in json.loads(logs.JsonFormatter().format(record))
