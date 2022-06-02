from http import HTTPStatus
from scielo_core.id_provider import (
    controller,
    xml_sps,
)
from scielo_core.basic.xml_sps_zip_file import get_xml_content


def request_document_id(pkg_file_path):
    arguments = xml_sps.IdRequestArguments(pkg_file_path)

    response = controller.request_document_ids(**arguments.data)

    if controller.is_diff(arguments.data, response):
        # return updated XML
        xml = get_xml_content(pkg_file_path)
        return xml.decode("utf-8")

    # return HTTP status code created (201)
    return HTTPStatus.CREATED
