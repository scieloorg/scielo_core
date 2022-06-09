import logging
import os
from zipfile import ZipFile, BadZipFile


LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def get_xml_content(xml_sps_file_path):
    """
    Get XML content from XML file or Zip file

    Arguments
    ---------
        xml_sps_file_path: str

    Return
    ------
    bytes
    """
    try:
        LOGGER.info(
            "Try to get xml content from zipfile %s" % xml_sps_file_path
        )
        with ZipFile(xml_sps_file_path) as zf:
            for item in zf.namelist():
                if item.endswith(".xml"):
                    return zf.read(item).decode("utf-8")
    except BadZipFile:
        LOGGER.info(
            "Try to get xml content from xml file %s" % xml_sps_file_path
        )
        with open(xml_sps_file_path, "rb") as fp:
            return fp.read().decode("utf-8")
    except Exception as e:
        LOGGER.exception("Unable to get_xml_content %s %s %s" %
                         (xml_sps_file_path, type(e), e))
    LOGGER.info("...Get xml content from %s" % xml_sps_file_path)


def update_zip_file_xml(xml_sps_file_path, content):
    """
    Save XML content in a Zip file.
    Return saved zip file path

    Arguments
    ---------
        xml_sps_file_path: str
        content: bytes

    Return
    ------
    str
    """
    try:
        LOGGER.info("Try to read zip %s" % xml_sps_file_path)
        with ZipFile(xml_sps_file_path) as zf:
            for item in zf.namelist():
                if item.endswith(".xml"):
                    xml_file_path = item
                    break
    except BadZipFile:
        LOGGER.info("Try to read xml %s" % xml_sps_file_path)
        xml_file_path = os.path.basename(xml_sps_file_path)
        xml_sps_file_path = xml_sps_file_path + ".zip"

    with ZipFile(xml_sps_file_path, "w") as zf:
        zf.writestr(xml_file_path, content)
        LOGGER.info("Try to write xml %s %s %s" %
                    (xml_sps_file_path, xml_file_path, content[:100]))

    return xml_sps_file_path


def create_xml_zip_file(xml_sps_file_path, content):
    """
    Save XML content in a Zip file.
    Return saved zip file path

    Arguments
    ---------
        xml_sps_file_path: str
        content: bytes

    Return
    ------
    str
    """
    dirname = os.path.dirname(xml_sps_file_path)
    if dirname and not os.path.isdir(dirname):
        os.makedirs(dirname)

    basename = os.path.basename(xml_sps_file_path)
    name, ext = os.path.splitext(basename)

    with ZipFile(xml_sps_file_path, "w") as zf:
        zf.writestr(name + ".xml", content)
    return os.path.isfile(xml_sps_file_path)
