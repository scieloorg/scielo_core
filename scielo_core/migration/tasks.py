import os
import logging
import requests
from tempfile import mkdtemp

from celery import Celery

from scielo_core.config import (
    CELERY_RESULT_BACKEND_URL,
    CELERY_BROKER_URL,
    EXAMPLE_QUEUE,
    REGISTER_MIGRATION_QUEUE,
    GET_XML_FILE_URI_AND_PID_V3_QUEUE,
    GET_JOURNAL_SORTED_PIDS_QUEUE,
    CREATE_JOURNAL_XML_ZIPS_QUEUE,
    SAVE_XML_ZIP_FILE_QUEUE,
    GET_XML_FILE_CONTENT_QUEUE,
    CREATE_XML_ZIP_FILE_QUEUE,
)
from scielo_core.basic import xml_sps_zip_file
from scielo_core.id_provider import xml_sps
from scielo_core.migration import controller
from scielo_core.id_provider.view import request_document_id


app = Celery('tasks', backend=CELERY_RESULT_BACKEND_URL, broker=CELERY_BROKER_URL)

LOGGER = logging.getLogger(__name__)

DEFAULT_QUEUE = 'high_priority'


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

def register_migration(data, skip_update, get_result=None):
    res = task_register_migration.apply_async(
        queue=REGISTER_MIGRATION_QUEUE,
        args=(data, skip_update, ),
    )
    return _handle_result("task register_migration", res, get_result)


@app.task(name='task_register_migration')
def task_register_migration(data, skip_update):
    try:
        controller.save_migration(
            data["v2"], data["aop_pid"], data["file_path"],
            data["issn"],
            data["year"],
            data["order"],
            data["v91"],
            data["v93"],
            skip_update=skip_update,
        )
        return True
    except Exception as e:
        raise
        LOGGER.exception(
            "Unable to save migration %s: %s %s" %
            (data, type(e), e)
        )
        return False


###########################################
def update_migration(v2, pid_v3, xml_zip_file_path, get_result=None):
    res = task_update_migration.apply_async(
        queue=REGISTER_MIGRATION_QUEUE,
        args=(v2, pid_v3, xml_zip_file_path, ),
    )
    return _handle_result("task update_migration", res, get_result)


@app.task(name='task_update_migration')
def task_update_migration(v2, pid_v3, xml_zip_file_path):
    return _update_migration(v2, pid_v3, xml_zip_file_path)


def _update_migration(v2, pid_v3, xml_zip_file_path):
    try:
        controller.update_migration(v2, pid_v3, xml_zip_file_path)
    except Exception as e:
        LOGGER.exception(
            "Unable to update migration %s: %s %s" %
            (v2, type(e), e)
        )


###########################################

def harvest_journal_xmls(issn):
    res = task_harvest_journal_xmls.apply_async(
        queue=GET_JOURNAL_SORTED_PIDS_QUEUE,
        args=(issn, ),
    )
    return _handle_result("task harvest_journal_xmls", res, get_result=False)


@app.task(name='task_harvest_journal_xmls')
def task_harvest_journal_xmls(issn):
    for pid in controller.get_pids(issn, "GET_XML"):
        LOGGER.debug("Creating xml zip for %s" % pid)
        # gera o zip do xml obtido do website
        harvest_xml_and_update_migration(pid)


###########################################

def harvest_xml_and_update_migration(pid, get_result=None):
    res = task_harvest_xml_and_update_migration.apply_async(
        queue=SAVE_XML_ZIP_FILE_QUEUE,
        args=(pid, ),
    )
    return _handle_result("task harvest_xml_and_update_migration", res, get_result)


@app.task(name='task_harvest_xml_and_update_migration')
def task_harvest_xml_and_update_migration(pid):
    return _harvest_xml_and_update_migration(pid)


def _harvest_xml_and_update_migration(pid):
    data = _get_xml_file_uri_and_pid_v3(pid)
    try:
        uri = data["xml"]
        pid_v3 = data["v3"]
    except KeyError:
        LOGGER.error("Unable to get xml uri for %s" % pid)
        return None

    content = _get_xml_file_content(uri)
    if not content:
        LOGGER.error("Unable to get xml content for %s" % pid)
        return None

    try:
        xml_zip_file_path = _create_tmp_xml_zip_file(pid + ".zip", content)
        _update_migration(pid, pid_v3, xml_zip_file_path)
    except Exception as e:
        LOGGER.exception(
            "Unable to update_migration: %s %s %s" %
            (pid, type(e), e)
        )
    finally:
        _delete_temp_xml_zip_file_path(xml_zip_file_path)


# --------------------------------------
def get_xml_file_uri_and_pid_v3(pid):
    res = task_get_xml_file_uri_and_pid_v3.apply_async(
        queue=GET_XML_FILE_URI_AND_PID_V3_QUEUE,
        args=(pid, ),
    )
    return _handle_result("task get_xml_file_uri_and_pid_v3", res, get_result=True)


@app.task(name='task_get_xml_file_uri_and_pid_v3')
def task_get_xml_file_uri_and_pid_v3(pid):
    return _get_xml_file_uri_and_pid_v3(pid)


def _get_xml_file_uri_and_pid_v3(pid):
    try:
        article = controller.get_article(pid)
        return {"xml": article.xml, "v3": article._id}
    except controller.FetchArticleError as e:
        LOGGER.exception(
            "Unable to get article data %s: %s %s" % (pid, type(e), e))
        return None


# --------------------------------------
def get_xml_file_content(uri):
    res = task_get_xml_file_content.apply_async(
        queue=GET_XML_FILE_CONTENT_QUEUE,
        args=(uri, ),
    )
    return _handle_result("task get_xml_file_content", res, get_result=True)


@app.task(name='task_get_xml_file_content')
def task_get_xml_file_content(uri):
    return _get_xml_file_content(uri)


def _get_xml_file_content(uri, timeout=None):
    timeout = timeout or 10
    try:
        r = requests.get(uri, timeout=timeout)
    except requests.Timeout as e:
        LOGGER.exception(
            "Try to get XML content again %s: %s" % (uri, timeout))
        return _get_xml_file_content(uri, timeout=timeout*2)
    except requests.HTTPError as e:
        LOGGER.exception(
            "Unable to get XML content %s: %s %s" % (uri, type(e), e))
        return None
    else:
        return r.text


# --------------------------------------

def _create_tmp_xml_zip_file(filename, xml_content):
    try:
        LOGGER.info("Creating tmp xml zip file: %s" % xml_content[:100])
        xml_sps.is_well_formed(xml_content)
        tempdir = mkdtemp()
        xml_zip_file_path = os.path.join(tempdir, filename)
        LOGGER.info("Creating tmp xml zip file: %s" % xml_zip_file_path)
        xml_sps_zip_file.create_xml_zip_file(xml_zip_file_path, xml_content)
        LOGGER.info("Creating tmp xml zip file: %s" % xml_zip_file_path)
        return xml_zip_file_path
    except IOError as e:
        if os.path.isdir(tempdir):
            _delete_temp_dir(tempdir)
        LOGGER.exception(
            "Unable to create XML ZIP file %s: %s %s" %
            (filename, type(e), e))
        return None


#############################################################

def migrate_journal_xmls(issn):
    for pid in controller.get_pids(issn, order_by="v2", status="TO_MIGRATE"):
        LOGGER.debug("Migrate %s" % pid)
        # gera o zip do xml obtido do website
        migrate_xml(pid)


# --------------------------------------
def migrate_xml(pid):
    res = task_migrate_xml.apply_async(
        queue=GET_XML_FILE_URI_AND_PID_V3_QUEUE,
        args=(pid, ),
    )
    return _handle_result("task migrate_xml", res, get_result=True)


@app.task(name='task_migrate_xml')
def task_migrate_xml(pid):
    migration = controller.get_migration(pid)
    if not migration:
        LOGGER.exception(
            "Unable to get article data %s" % (pid, ))
        return None

    try:
        xml_zip_file_path = _create_tmp_xml_zip_file(
            pid + ".zip", migration.zip_file)
        
        resp = request_document_id(xml_zip_file_path)
    except Exception as e:
        LOGGER.exception(
            "Unable to request_document_id: %s %s %s" %
            (pid, type(e), e)
        )
    else:
        try:
            migration.status = "MIGRATED"
            migration.save()
        except Exception as e:
            LOGGER.exception(
                "Unable to migrate %s: %s %s" % (pid, type(e), e))
    finally:
        _delete_temp_xml_zip_file_path(xml_zip_file_path)


def _delete_temp_xml_zip_file_path(xml_zip_file_path):
    return
    try:
        os.unlink(xml_zip_file_path)
    except IOError:
        LOGGER.exception("Unable to delete %s" % xml_zip_file_path)

    tempdir = os.path.dirname(xml_zip_file_path)
    _delete_temp_dir(tempdir)


def _delete_temp_dir(tempdir):
    try:
        os.rmdir(tempdir)
    except IOError:
        LOGGER.exception("Unable to delete %s" % tempdir)
