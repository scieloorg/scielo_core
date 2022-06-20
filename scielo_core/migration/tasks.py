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
    UNDO_ID_REQUEST_QUEUE,
    get_article_meta_uri,
)
from scielo_core.basic import xml_sps_zip_file
from scielo_core.id_provider import (
    xml_sps,
    exceptions,
)
from scielo_core.id_provider.controller import get_xml_by_v2
from scielo_core.migration import controller
from scielo_core.id_provider.view import request_document_id, HTTPStatus


app = Celery('tasks', backend=CELERY_RESULT_BACKEND_URL, broker=CELERY_BROKER_URL)

LOGGER = logging.getLogger(__name__)

DEFAULT_QUEUE = 'high_priority'


class UnableToCreateXMLZipFileError(Exception):
    ...


class PullDataFromNewWebsiteError(Exception):
    ...


class PullXMLError(Exception):
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

def _request_xml(uri, timeout=None):
    timeout = timeout or 10
    try:
        r = requests.get(uri, timeout=timeout)
    except requests.Timeout as e:
        LOGGER.debug(
            "Try to request %s again: %s" % (uri, timeout))
        return _request_xml(uri, timeout=timeout*2)
    except requests.HTTPError as e:
        raise PullXMLError(
            "Unable to get XML content %s: %s %s" % (uri, type(e), e))
    else:
        if r.status_code != 200:
            raise PullXMLError("For %s, got %s" % (uri, r.status_code))
        return r.text


def _pull_xml(pid, xml_folder_path, collection):
    data = {}

    migration = controller.get_migration(pid)

    if xml_folder_path:
        # get xml from a xml_folder_path
        try:
            data = _get_data_from_old_website_file_system(
                xml_folder_path, migration.file_path)
            return data
        except PullXMLError as e:
            data['xml'] = None
            data['source'] = xml_folder_path

    if collection:
        # get xml from a collection
        try:
            data = _get_data_from_am(pid, collection)
            return data
        except PullXMLError as e:
            data['xml'] = None
            data['source'] = collection

    if not xml_folder_path and not collection:
        try:
            # get xml from new website
            data = _get_data_from_new_website(pid)
            return data
        except PullDataFromNewWebsiteError as e:
            data['xml'] = None
            data['source'] = 'new_website'

    return data


def _get_xml_file_uri_and_pid_v3(pid):
    try:
        article = controller.get_article(pid)
    except (controller.FetchArticleError) as e:
        raise PullDataFromNewWebsiteError(
            "Unable to get article data %s: %s %s" % (pid, type(e), e))

    try:
        return {"xml": article.xml, "v3": article._id}
    except (AttributeError) as e:
        raise PullDataFromNewWebsiteError(
            "Unable to get article data %s: %s %s" % (pid, type(e), e))


def _get_data_from_new_website(pid):
    """
    Raises
    ------
    PullDataFromNewWebsiteError
    PullXMLError
    """
    data = _get_xml_file_uri_and_pid_v3(pid)
    uri = data["xml"]
    pid_v3 = data["v3"]
    content = _request_xml(uri)
    return {"source": uri, "v3": pid_v3, "xml": content}


def _get_data_from_old_website_file_system(xml_folder_path, file_path):
    """
    Raises
    ------
    PullXMLError
    """
    try:
        file_path = os.path.join(xml_folder_path, file_path)
        with open(file_path) as fp:
            content = fp.read()
            return {"source": file_path, "xml": content}
    except IOError as e:
        raise PullXMLError(e)


def _get_data_from_am(pid, collection):
    """
    Raises
    ------
    PullXMLError
    """
    uri = get_article_meta_uri(pid, collection)
    return {"source": uri, "xml": _request_xml(uri)}


def push_xml_zip_file(xml_zip_file_path):
    try:
        LOGGER.debug("zip %s" % xml_zip_file_path)

        resp = request_document_id(xml_zip_file_path, "migration")

        _delete_temp_xml_zip_file_path(xml_zip_file_path)

        LOGGER.debug("resp=%s" % resp)
        if resp == HTTPStatus.INTERNAL_SERVER_ERROR:
            raise exceptions.RequestDocumentIdError

    except Exception as e:
        LOGGER.debug("Error `push_xml_zip_file` %s" % e)
        _delete_temp_xml_zip_file_path(xml_zip_file_path)
        raise exceptions.RequestDocumentIdError


#############################################
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

def pull_data_and_request_id(issn, xml_folder_path, collection):
    res = task_pull_data_and_request_id.apply_async(
        queue=HARVEST_XMLS_QUEUE,
        args=(issn, xml_folder_path, collection),
    )
    return _handle_result("task pull_data_and_request_id", res, get_result=False)


@app.task(name='task_pull_data_and_request_id')
def task_pull_data_and_request_id(issn, xml_folder_path, collection):
    for is_aop in (True, False):
        for pid in controller.get_pids(
                issn, "CREATED", is_aop, order_by="pid"):
            _pull_data_and_request_id(pid, xml_folder_path, collection)


def _pull_data_and_request_id(pid, xml_folder_path, collection):
    try:
        LOGGER.debug("Pull data %s" % pid)
        data = _pull_xml(pid, xml_folder_path, collection)

        controller.add_xml_and_v3(
            pid, data.get("v3"), data["xml"], data["source"])

        if not data['xml']:
            raise PullXMLError("Unable to get XML %s" % pid)

        _request_id_and_update_migration(pid, data["xml"])

    except (
            PullDataFromNewWebsiteError,
            PullXMLError,
            ) as e:
        LOGGER.error("Unable to harvest data %s %s" % (pid, e))
        migration = controller.get_migration(pid)
        controller.update_status(migration, None, "Not found %s" % pid)
        return None
    except (
            controller.SaveMigrationError,
            ) as e:
        LOGGER.exception("Unable to update migration data %s %s" % (pid, e))
        return None

#############################################################


def request_id_for_journal_documents(issn):
    res = task_request_id_for_journal_documents.apply_async(
        queue=MIGRATE_XMLS_QUEUE,
        args=(issn, ),
    )
    return _handle_result(
        "task request_id_for_journal_documents", res, get_result=False)


@app.task(name='task_request_id_for_journal_documents')
def task_request_id_for_journal_documents(issn):
    for is_aop in (True, False):
        for pid in controller.get_pids(
                issn, "XML", is_aop, order_by="pid"):
            res = _request_id_and_update_migration(pid)


def _request_id_and_update_migration(pid, xml=None):
    try:

        LOGGER.debug("Migrate %s" % pid)
        migration = controller.get_migration(pid)

    except controller.FetchMigrationError as e:
        LOGGER.exception("Migration %s not found" % pid)
        return

    try:
        xml = xml or migration.xml
        if not xml:
            raise exceptions.InvalidXMLError("Article %s has no XML" % pid)

        LOGGER.debug("request_document_id %s" % xml[:10])

        xml_zip_file_path = _create_tmp_xml_zip_file(f"{pid}.zip", xml)

        push_xml_zip_file(xml_zip_file_path)
        status = "MIGRATED"
        status_msg = ""

    except exceptions.RequestDocumentIdError as e:
        status_msg = str(e)
        status = "XML"

    except exceptions.InvalidXMLError as e:
        LOGGER.debug("_request_id_and_update_migration %s" % e)
        return

    controller.update_status(migration, status, status_msg)


#############################################################


def undo_id_request_for_journal_documents(issn):
    res = task_undo_id_request_for_journal_documents.apply_async(
        queue=UNDO_ID_REQUEST_QUEUE,
        args=(issn, ),
    )
    return _handle_result(
        "task undo_id_request_for_journal_documents", res, get_result=False)


@app.task(name='task_undo_id_request_for_journal_documents')
def task_undo_id_request_for_journal_documents(issn):
    for pid in controller.get_pids(issn, "MIGRATED"):
        res = _undo_id_request_and_update_migration(pid)


def _undo_id_request_and_update_migration(pid):
    try:

        LOGGER.debug("Undo id request %s" % pid)
        xml = get_xml_by_v2(pid)

    except Exception as e:
        LOGGER.exception(
            "Unable to undo id request, XML %s not found. %s" % (pid, e))
        return
    if xml:
        try:
            LOGGER.debug("Get migration %s" % pid)
            migration = controller.get_migration(pid)

        except controller.FetchMigrationError as e:
            LOGGER.exception("Migration %s not found" % pid)
            return

        migration.xml = xml
        migration.status = "XML"
        migration.status_msg = "id request undone"
        controller.update_status(migration, "XML", "id request undone")


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
    except (IOError, TypeError):
        LOGGER.exception("Unable to delete %s" % xml_zip_file_path)

    tempdir = os.path.dirname(xml_zip_file_path)
    _delete_temp_dir(tempdir)


def _delete_temp_dir(tempdir):
    try:
        os.rmdir(tempdir)
    except IOError:
        LOGGER.exception("Unable to delete %s" % tempdir)
