import os
from zipfile import ZipFile, BadZipFile


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
        # xml_sps_file_path is a zipfile
        zf = xml_sps_file_path
        namelist = zf.namelist()
    except AttributeError:
        try:
            with ZipFile(xml_sps_file_path) as zf:
                for item in zf.namelist():
                    if item.endswith(".xml"):
                        return zf.read(item)
        except BadZipFile:
            with open(xml_sps_file_path, "rb") as fp:
                return fp.read()
    else:
        for item in namelist:
            if item.endswith(".xml"):
                return zf.read(item)


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
        with ZipFile(xml_sps_file_path) as zf:
            for item in zf.namelist():
                if item.endswith(".xml"):
                    xml_file_path = item
                    break
    except BadZipFile:
        xml_file_path = os.path.basename(xml_sps_file_path)
        xml_sps_file_path = xml_sps_file_path + ".zip"

    with ZipFile(xml_sps_file_path, "wb") as zf:
        zf.writestr(xml_file_path, content)
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

    with ZipFile(xml_sps_file_path, "wb") as zf:
        zf.writestr(name + ".xml", content)
    return os.path.isfile(xml_sps_file_path)
