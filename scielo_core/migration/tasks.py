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
    HARVEST_XMLS_QUEUE,
    MIGRATE_XMLS_QUEUE,
)
from scielo_core.basic import xml_sps_zip_file
from scielo_core.id_provider import xml_sps, exceptions
from scielo_core.migration import controller
from scielo_core.id_provider.view import request_document_id, HTTPStatus


app = Celery('tasks', backend=CELERY_RESULT_BACKEND_URL, broker=CELERY_BROKER_URL)

LOGGER = logging.getLogger(__name__)

DEFAULT_QUEUE = 'high_priority'


class UnableToCreateXMLZipFileError(Exception):
    ...


class HarvestingArticleDataError(Exception):
    ...

class HarvestingArticlXMLError(Exception):
    ...


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
        controller.create_migration(
            data["v2"], data["aop_pid"], data["file_path"],
            data["issn"],
            data["year"],
            data["order"],
            data["v91"],
            data["v93"],
            data["is_aop"],
            skip_update=skip_update,
        )
        return True
    except controller.SaveMigrationError as e:
        LOGGER.exception(
            "Unable to save migration %s: %s %s" %
            (data, type(e), e)
        )
        return False


###########################################

def harvest_journal_xmls(issn):
    res = task_harvest_journal_xmls.apply_async(
        queue=HARVEST_XMLS_QUEUE,
        args=(issn, ),
    )
    return _handle_result("task harvest_journal_xmls", res, get_result=False)


@app.task(name='task_harvest_journal_xmls')
def task_harvest_journal_xmls(issn):
    for pid in controller.get_pids(issn, "GET_XML"):
        LOGGER.debug("Creating xml zip for %s" % pid)
        # gera o zip do xml obtido do website
        _harvest_xml_and_update_migration(pid)


def _harvest_xml_and_update_migration(pid):
    try:
        data = _get_xml_file_uri_and_pid_v3(pid)
        uri = data["xml"]
        pid_v3 = data["v3"]
        content = _get_xml_file_content(uri)
        controller.add_xml_and_v3(pid, pid_v3, content)
    except (
            HarvestingArticleDataError,
            HarvestingArticlXMLError,
            ) as e:
        LOGGER.error("Unable to harvest data %s %s" % (pid, e))
        return None
    except (
            controller.SaveMigrationError,
            ) as e:
        LOGGER.exception("Unable to update migration data %s %s" % (pid, e))
        return None


def _get_xml_file_uri_and_pid_v3(pid):
    try:
        article = controller.get_article(pid)
        return {"xml": article.xml, "v3": article._id}
    except controller.FetchArticleError as e:
        raise HarvestingArticleDataError(
            "Unable to get article data %s: %s %s" % (pid, type(e), e))


def _get_xml_file_content(uri, timeout=None):
    timeout = timeout or 10
    try:
        r = requests.get(uri, timeout=timeout)
    except requests.Timeout as e:
        LOGGER.debug(
            "Try to request %s again: %s" % (uri, timeout))
        return _get_xml_file_content(uri, timeout=timeout*2)
    except requests.HTTPError as e:
        raise HarvestingArticlXMLError(
            "Unable to get XML content %s: %s %s" % (uri, type(e), e))
    else:
        return r.text


#############################################################


def migrate_journal_xmls(issn):
    res = task_migrate_journal_xmls.apply_async(
        queue=MIGRATE_XMLS_QUEUE,
        args=(issn, ),
    )
    return _handle_result("task migrate_journal_xmls", res, get_result=False)


@app.task(name='task_migrate_journal_xmls')
def task_migrate_journal_xmls(issn):
    for is_aop in (True, False):
        for pid in controller.get_pids(
                issn, "TO_MIGRATE", is_aop, order_by="pid"):
            LOGGER.debug("Migrate %s %s" % (pid, is_aop))
            res = _migrate_xml(pid)


def _migrate_xml(pid):
    status = "TO_MIGRATE"
    status_msg = ""

    xml_zip_file_path = None
    try:
        migration = controller.get_migration(pid)
        xml_zip_file_path = _create_tmp_xml_zip_file(
            f"{pid}.zip", migration.xml)
        resp = request_document_id(xml_zip_file_path, "migration")
        if resp == HTTPStatus.INTERNAL_SERVER_ERROR:
            raise exceptions.RequestDocumentIdError

        status = "MIGRATED"

    except (
            UnableToCreateXMLZipFileError,
            exceptions.RequestDocumentIdError,
            ) as e:
        status_msg = str(e)
        LOGGER.exception(
            "???? migrate xml %s %s %s" %
            (pid, type(e), str(e))
        )

    try:
        controller.update_status(migration, status, status_msg)
    except Exception as e:
        LOGGER.exception(
            "Unable to migrate xml %s %s %s" %
            (pid, type(e), str(e))
        )
    if xml_zip_file_path:
        _delete_temp_xml_zip_file_path(xml_zip_file_path)


#############################################################

def _create_tmp_xml_zip_file(filename, xml_content):
    try:
        LOGGER.debug("Creating tmp xml zip file: %s %s" %
                     (filename, xml_content[:100]))
        LOGGER.debug("Validate XML")
        xml_sps.is_valid_xml(xml_content)

        LOGGER.debug("Creating file")
        tempdir = mkdtemp()
        xml_zip_file_path = os.path.join(tempdir, filename)
        xml_sps_zip_file.create_xml_zip_file(xml_zip_file_path, xml_content)
        LOGGER.debug("Created: %s" % xml_zip_file_path)
        return xml_zip_file_path
    except xml_sps.etree.XMLSyntaxError as e:
        raise UnableToCreateXMLZipFileError(
            "Unable to create XML ZIP file %s: %s %s" %
            (filename, type(e), e))
    except Exception as e:
        _delete_temp_dir(tempdir)
        raise UnableToCreateXMLZipFileError(
            "Unable to create XML ZIP file %s: %s %s" %
            (filename, type(e), e))


def _delete_temp_xml_zip_file_path(xml_zip_file_path):
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
