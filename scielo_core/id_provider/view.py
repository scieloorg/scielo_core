import logging

from http import HTTPStatus

from scielo_core import config
from scielo_core.id_provider import (
    controller,
    exceptions,
)


LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(
    filename=config.SCIELO_CORE_ID_PROVIDER_LOGFILE, level=logging.DEBUG)


def request_document_id(pkg_file_path, username):
    print(pkg_file_path, username)
    try:
        LOGGER.debug("request_document_id %s %s" % (pkg_file_path, username))
        response = controller.request_document_ids_from_file(
            pkg_file_path, username)
    except (exceptions.InvalidXMLError, exceptions.InputDataError):
        return HTTPStatus.BAD_REQUEST
    except exceptions.SaveError:
        return HTTPStatus.INTERNAL_SERVER_ERROR
    else:
        return response or HTTPStatus.CREATED


def get_xml(v3):
    return controller.get_xml(v3)
