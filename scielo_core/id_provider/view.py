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
    filename=config.ID_PROVIDER_LOGFILE, encoding='utf-8', level=logging.DEBUG)


def request_document_id(pkg_file_path, username):
    print(pkg_file_path, username)
    try:
        LOGGER.debug("request_document_id %s %s" % (pkg_file_path, username))
        response = controller.request_document_ids(
            pkg_file_path, username)
    except exceptions.RequestDocumentIdError as e:
        LOGGER.debug(e)
        return HTTPStatus.INTERNAL_SERVER_ERROR
    except exceptions.DocumentIsUpdatedError as e:
        LOGGER.debug(e)
        return HTTPStatus.CREATED
    else:
        return response


def get_xml(v3):
    return controller.get_xml(v3)
