import logging

from celery import Celery

from scielo_core.id_provider import view
from scielo_core.config import (
    SCIELO_CORE_ID_PROVIDER_CELERY_RESULT_BACKEND_URL,
    SCIELO_CORE_ID_PROVIDER_CELERY_BROKER_URL,
)

EXAMPLE_QUEUE = 'low_priority'


app = Celery('tasks',
             backend=SCIELO_CORE_ID_PROVIDER_CELERY_RESULT_BACKEND_URL,
             broker=SCIELO_CORE_ID_PROVIDER_CELERY_BROKER_URL)

LOGGER = logging.getLogger(__name__)

REQUEST_QUEUE = 'high_priority'


def _handle_result(task_name, result, get_result):
    if get_result:
        return result.get()


###########################################

def example(data, get_result=None):
    res = task_example.apply_async(
        queue=EXAMPLE_QUEUE,
        args=(data, ),
    )
    return _handle_result("task example", res, get_result)


@app.task()
def task_example(data):
    return {"task": "example", "result": "done", "data": data}


###########################################

def request_id(pkg_file_path, get_result=None):
    res = task_request_id.apply_async(
        queue=REQUEST_QUEUE,
        args=(pkg_file_path, ),
    )
    return _handle_result("task request_id", res, get_result)


@app.task(name='request_id')
def task_request_id(pkg_file_path):
    return view.request_document_id(pkg_file_path)

