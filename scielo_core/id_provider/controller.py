import logging
from datetime import datetime
from http import HTTPStatus

from scielo_core.basic import mongo_db, xml_sps_zip_file
from scielo_core.basic.exceptions import InvalidXMLError
from scielo_core.id_provider import (
    models,
    exceptions,
    v3_gen,
    xml_sps,
)
from scielo_core.config import SCIELO_CORE_ID_PROVIDER_DB_URI


conn = mongo_db.mk_connection(SCIELO_CORE_ID_PROVIDER_DB_URI, 'scielo_core')

LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def get_xml_by_v2(v2):
    try:
        doc = _fetch_most_recent_document(v2=v2)
        return doc.xml
    except exceptions.FetchMostRecentRecordError:
        return


def get_xml(v3):
    try:
        doc = _fetch_most_recent_document(v3=v3)
        return doc.xml
    except exceptions.FetchMostRecentRecordError:
        return


def request_document_ids_from_file(pkg_file_path, user=None):
    LOGGER.debug(pkg_file_path)

    try:
        arguments = xml_sps.IdRequestArguments(pkg_file_path)
    except InvalidXMLError as e:
        raise exceptions.InvalidXMLError(
            "Invalid XML in %s: %s" % (pkg_file_path, e)
        )

    try:
        params = arguments.data
        params['user'] = user
        changed_input_xml = request_document_ids(**params)
        if changed_input_xml:
            xml_sps_zip_file.update_zip_file_xml(
                pkg_file_path, changed_input_xml)
        return changed_input_xml
    except (
            exceptions.InputDataError,
            exceptions.QueryingDocumentInIssueError,
            exceptions.QueryingDocumentAsAOPError,
            exceptions.FetchMostRecentRecordError,
            exceptions.NotEnoughParametersToGetDocumentRecordError,
            exceptions.GetRegisteredDocumentError,
            ) as e:
        raise exceptions.InputDataError(e)

    except (
            exceptions.InvalidXMLError,
            exceptions.PrepareDataToSaveError,
            exceptions.SavingError,
            ) as e:
        raise exceptions.SavingError(e)


def request_document_ids(
        v2, v3, aop_pid,
        doi_with_lang,
        issns,
        pub_year,
        volume, number, suppl,
        elocation_id, fpage, fpage_seq, lpage,
        authors, collab,
        article_titles,
        partial_body,
        zip_file_path,
        extra=None,
        user=None):
    """
    Request Document PIDs

    Parameters
    ----------
    v2: str
    v3: str
    aop_pid: str
    doi_with_lang: {"lang": str, "value": str} list
    issns: {"type": str, "value": str} list
    pub_year: str
    volume: str
    number: str
    suppl: str
    elocation_id: str
    fpage: str
    fpage_seq: str
    lpage: str
    authors: {"surname": str, "given_names": str, "suffix": str, "prefix": str, "orcid": str} list
    collab: str
    article_titles: {"lang": str, "text": str} list
    partial_body: str
    zip_file_path: str
    extra: dict

    Returns
    -------
    None or str
    None, if there was no XML changes
    str, XML changed

    Raises
    ------
    exceptions.InputDataError
    exceptions.QueryingDocumentInIssueError
    exceptions.QueryingDocumentAsAOPError
    exceptions.FetchMostRecentRecordError
    exceptions.NotEnoughParametersToGetDocumentRecordError
    exceptions.DocumentDoesNotExistError
    exceptions.GetRegisteredDocumentError
    exceptions.InvalidXMLError
    exceptions.PrepareDataToSaveError
    exceptions.SavingError
    """

    # obtém os dados de entrada
    input_data = _get_document_input_data(
        v2, v3, aop_pid,
        doi_with_lang,
        issns,
        pub_year,
        volume, number, suppl,
        elocation_id, fpage, fpage_seq, lpage,
        authors, collab,
        article_titles,
        partial_body,
        zip_file_path,
        extra,
    )

    # registra a requisição de pids
    request = _log_new_request(input_data, user)

    # obtém o registro do documento
    try:
        registered = _get_registered_document(input_data)
    except exceptions.DocumentDoesNotExistError:
        registered = None

    # obtém os dados do documento registrado
    registered_data = (registered and registered.as_dict()) or {}

    changed_input_xml = None
    if _pids_updated(input_data, registered_data):
        # verifica se o XML de entrada foi modificado com os
        # pids recuperados / gerados
        changed_input_xml = input_data['xml']

    # prepara os dados para gravar
    pkg = _prepare_data_to_register(input_data, registered)

    # grava
    _register_document(pkg)

    # registra a atualização da requisição
    _log_request_update(request, input_data)

    return changed_input_xml


def _get_document_input_data(
        v2, v3, aop_pid,
        doi_with_lang,
        issns,
        pub_year,
        volume, number, suppl,
        elocation_id, fpage, fpage_seq, lpage,
        authors, collab,
        article_titles,
        partial_body,
        zip_file_path,
        extra=None,
        ):
    """
    Retorna os dados do documento de entrada em formato de dicionario

    Parameters
    ----------
    v2: str
    v3: str
    aop_pid: str
    doi_with_lang: {"lang": str, "value": str} list
    issns: {"type": str, "value": str} list
    pub_year: str
    volume: str
    number: str
    suppl: str
    elocation_id: str
    fpage: str
    fpage_seq: str
    lpage: str
    authors: {"surname": str, "given_names": str, "suffix": str, "prefix": str, "orcid": str} list
    collab: str
    article_titles: {"lang": str, "text": str} list
    partial_body: str
    zip_file_path: str
    extra: dict

    Returns
    -------
    dict

    Raises
    ------
    exceptions.InputDataError

    """
    document = Document(
        v2, v3, aop_pid,
        doi_with_lang,
        issns,
        pub_year,
        volume, number, suppl,
        elocation_id, fpage, fpage_seq, lpage,
        authors, collab,
        article_titles,
        partial_body,
        zip_file_path,
        extra,
    )
    return document.attribs


def _log_request_update(request, data):
    try:
        request.out_v2 = data['v2']
        request.out_v3 = data['v3']
        request.out_aop_pid = data['aop_pid']
        request.status = "requested"
        request.update_diffs()
        request.save()
        LOGGER.debug("Update request done")
    except Exception as e:
        LOGGER.debug("Error: Update request done")


def _log_new_request(data, user):
    try:
        request = models.Requests()
        request.user = user
        request.in_v2 = data['v2']
        request.in_v3 = data['v3']
        request.in_aop_pid = data['aop_pid']
        request.status = "request"
        request.save()
        LOGGER.debug("Registered request")
    except Exception as e:
        request = None
        LOGGER.debug("Error: Register request")
    return request


def _pids_updated(input_data, registered_data):
    """
    Check `input_data` pids were changed

    Parameters
    ----------
    input_data: dict
    registered_data: dict

    Returns
    -------
    bool

    Raises
    ------
    exceptions.InvalidXMLError

    """
    LOGGER.debug("Input data: %s" % input_data)

    # obtém os pids dos dados de entrada
    ids = _get_ids(input_data)

    # adiciona os pids faltantes aos dados de entrada
    _add_pids(input_data, registered_data)

    # obtém os pids atualizados dos dados de entrada
    new_ids = _get_ids(input_data)

    # read xml
    input_data['xml'] = xml_sps.get_xml_content(input_data['zip_file_path'])

    if ids != new_ids:
        # update xml with pids
        input_data["xml"] = xml_sps.update_ids(
            input_data["xml"],
            input_data["v3"],
            input_data["v2"],
            input_data["aop_pid"],
        )
        return True
    else:
        return False


def _add_pids(input_data, registered_data):
    """
    Atualiza input_data com IDs

    Arguments
    ---------
    input_data: dict
    registered_data: dict

    Returns
    -------
    dict
    """
    input_data = _add_pid_v3(input_data, registered_data)
    input_data = _add_aop_pid(input_data, registered_data)
    input_data = _add_pid_v2(input_data, registered_data)

    return input_data


class Document:

    def __init__(self, v2, v3, aop_pid,
                 doi_with_lang,
                 issns,
                 pub_year,
                 volume, number, suppl,
                 elocation_id, fpage, fpage_seq, lpage,
                 authors, collab,
                 article_titles,
                 partial_body,
                 zip_file_path,
                 extra=None
                 ):

        document_data = {}
        document_data["v3"] = v3 or ''

        # outros tipos de ID
        document_data["v2"] = (v2 or '').upper()
        document_data["aop_pid"] = (aop_pid or '').upper()

        # dados que identificam o documento e que sempre estão presentes
        try:
            document_data["issns"] = [
                {"type": item["type"], "value": item["value"].upper()}
                for item in issns
            ]
        except KeyError as e:
            raise exceptions.InputDataError(
                "issns must have type and value: %s %s" %
                (issns, e)
            )

        document_data["pub_year"] = pub_year.upper()

        # dados que identificam o documento e não são obrigatórios
        try:
            document_data["doi_with_lang"] = [
                {"lang": item["lang"], "value": item["value"].upper()}
                for item in doi_with_lang
            ]
        except KeyError as e:
            raise exceptions.InputDataError(
                "doi_with_lang must have lang and value: %s %s" %
                (doi_with_lang, e))

        # authors and collab
        document_data["collab"] = (collab or '').upper()

        try:
            document_data["authors"] = [
                {
                    "surname": item["surname"].upper(),
                    "given_names": item["given_names"],
                    "prefix": item.get("prefix") or '',
                    "suffix": item.get("suffix") or '',
                    "orcid": item.get("orcid") or '',
                }
                for item in authors
            ]
        except KeyError as e:
            raise exceptions.InputDataError(
                "authors must have surname and given_names: %s %s" %
                (authors, e))

        # títulos do documento
        try:
            document_data["article_titles"] = [
                {"lang": item["lang"], "text": item["text"].upper()}
                for item in article_titles
            ]
        except KeyError as e:
            raise exceptions.InputDataError(
                "article_titles must have lang and text: %s %s" %
                (article_titles, e))

        # dados complementares que identificam o documento
        document_data["volume"] = (volume or '').upper()
        document_data["number"] = (number or '').upper()
        document_data["suppl"] = (suppl or '').upper()
        document_data["elocation_id"] = (elocation_id or '').upper()
        document_data["fpage"] = (fpage or '').upper()
        document_data["fpage_seq"] = (fpage_seq or '').upper()
        document_data["lpage"] = (lpage or '').upper()

        # quando o documento não tem metadados suficientes para identificar
        document_data["partial_body"] = _standardize_partial_body(partial_body)

        # extra
        document_data["extra"] = extra or {}

        document_data["zip_file_path"] = zip_file_path

        self._attribs = document_data

    @property
    def attribs(self):
        return self._attribs


##############################################


def _get_registered_document(document_attribs):
    """
    Get registered document:
    - search expression with pid v2
    - search expression without pid v2
    - search expression with aop version

    Parameters
    ----------
    document: Document

    Returns
    -------
    Package

    Raises
    ------
    exceptions.QueryingDocumentInIssueError
    exceptions.QueryingDocumentAsAOPError
    exceptions.FetchMostRecentRecordError
    exceptions.NotEnoughParametersToGetDocumentRecordError
    exceptions.DocumentDoesNotExistError
    exceptions.GetRegisteredDocumentError
    """
    # busca o documento com dados do fascículo + pid v2
    LOGGER.debug("Find document with v2")
    registered = _get_document_published_in_an_issue(
        document_attribs, with_v2=True)
    if not registered:
        # busca o documento com dados do fascículo, sem pid v2
        LOGGER.debug("Find document without v2")
        registered = _get_document_published_in_an_issue(document_attribs)
    if not registered:
        # recupera dados de aop, se aplicável
        LOGGER.debug("Find document published as aop")
        registered = _get_document_published_as_aop(document_attribs)

    try:
        doc = registered.as_dict()
    except AttributeError:
        raise exceptions.DocumentDoesNotExistError(
            "Document does not exist %s" % document_attribs
        )

    try:
        return _fetch_most_recent_document(v3=doc["v3"])
    except KeyError as e:
        raise exceptions.GetRegisteredDocumentError(
            "Unable to get most recent document %s: %s" %
            (doc, e)
        )


def _get_ids(data):
    """
    Returns the document pids list

    Arguments
    ---------
    data: dict

    Returns
    -------
    list

    """
    pids = []
    if data:
        for k in ("v2", "v3", "aop_pid"):
            pids.append(data.get(k) or '')
    return pids


def _get_query_parameters(document_attribs, with_v2=False, aop_version=False):
    """
    Get query parameters

    Arguments
    ---------
    document: Document
    with_v2: bool
    aop_version: bool
    with_issue: bool

    Returns
    -------
    dict
    """
    params = {}

    for k in ("doi_with_lang", "authors", "collab", "article_titles"):
        if document_attribs[k]:
            break
        # nenhum destes, então procurar pelo início do body
        if not document_attribs["partial_body"]:
            raise exceptions.NotEnoughParametersToGetDocumentRecordError(
                str(document_attribs)
            )
        params["partial_body"] = document_attribs["partial_body"]

    for attr in ("pub_year", "collab", ):
        params[attr] = document_attribs[attr]

    for attr in ("volume", "number", "suppl", "elocation_id",
                 "fpage", "fpage_seq", "lpage", ):
        if aop_version:
            params[attr] = ''
        else:
            params[attr] = document_attribs[attr]

    if not aop_version and with_v2:
        params["v2"] = document_attribs["v2"]

    params["surnames"] = ""
    if document_attribs["authors"]:
        params["surnames"] = " ".join([
            author["surname"] for author in document_attribs["authors"]
        ])

    qs = None
    attributes = [
        ("issns", "value"),
        ("doi_with_lang", "value"),
        ("article_titles", "text"),
    ]
    for field, subfield in attributes:
        _params = mongo_db._get_EmbeddedDocumentListField_query_params(
            document_attribs[field], field, subfield
        )
        if _params.get("qs"):
            if qs:
                qs &= (_params["qs"])
            else:
                qs = _params["qs"]
        else:
            params.update(_params)
    if qs:
        params["qs"] = qs
    LOGGER.debug("Parameters %s" % str(params))
    return params


def _get_document_published_in_an_issue(document_attribs, with_v2=False):
    """
    Query document with issue data

    Arguments
    ---------
    document_attribs: dict
    with_v2: bool
        usa ou não o v2 na consulta
    """
    try:
        params = _get_query_parameters(document_attribs, with_v2=with_v2)
        return _fetch_most_recent_document(**params)
    except exceptions.FetchMostRecentRecordError as e:
        raise exceptions.QueryingDocumentInIssueError(
            f"Querying document in an issue error: {e}"
        )


def _get_document_published_as_aop(document_attribs):
    """
    Query document with aop data
        "issn", "pub_year",
        "doi",
        "authors",
        "article_titles",
        e
        "volume": "", "number": "", "suppl": "", "fpage": "", "lpage": ""

    Arguments
    ---------
    document_attribs: dict
    """
    try:
        params = _get_query_parameters(document_attribs, aop_version=True)
        return _fetch_most_recent_document(**params)
    except exceptions.FetchMostRecentRecordError as e:
        raise exceptions.QueryingDocumentAsAOPError(
            f"Querying document as aop error: {e}"
        )


def _add_aop_pid(input_data, registered_data):
    """
    Atualiza input_data com aop_pid se aplicável

    Arguments
    ---------
        input_data: dict
        registered_data: dict
        registered_data: dict

    Returns
    -------
        dict
    """
    if (registered_data and
            not registered_data.get("volume") and
            not registered_data.get("number") and
            not registered_data.get("suppl")):
        input_data["aop_pid"] = registered_data["v2"]
    return input_data


##############################################


def _add_pid_v2(input_data, registered_data):
    """
    Garante que input_data tenha um v2 inédito

    Arguments
    ---------
        input_data: dict

    Returns
    -------
        dict
    """
    if not input_data["v2"]:
        if registered_data:
            input_data["v2"] = registered_data["v2"]
        else:
            input_data["v2"] = _get_unique_v2(input_data)
    return input_data


def _get_unique_v2(input_data):
    """
    Generate v2 and return it only if it is new

    Returns
    -------
        str
    """
    issn_id = _get_issn_for_pid_v2
    if not issn_id:
        raise PidV2GenerationError(
            "Unable to create pid v2 because there is no ISSN: %s" %
            input_data
        )
    try:
        year = input_data['pub_year']
    except KeyError:
        raise PidV2GenerationError(
            "Unable to create pid v2 because there is no pub_year: %s" %
            input_data
        )

    while True:
        generated = _v2_generates(issn_id, year)
        if not _is_registered_v2(generated):
            return generated


def _get_issn_for_pid_v2(input_data):
    """
    Generate v2 and return it only if it is new

    Returns
    -------
        str
    """
    epub_issn = None
    ppub_issn = None
    for issn in input_data['issns']:
        if issn['type'] == 'epub':
            epub_issn = issn['value']
        elif issn['type'] == 'ppub':
            ppub_issn = issn['value']
    return epub_issn or ppub_issn


def _generate_v2_suffix():
    return str(datetime.now().timestamp()).replace(".", "")


def _v2_generates(issn_id, year):
    randomnumber = _generate_v2_suffix()
    randomnumber = randomnumber[5:] + "0" * 9
    return f"S{issn_id}{year}{randomnumber[:9]}"


def _add_pid_v3(input_data, registered_data):
    """
    Garante que input_data tenha um v3 inédito

    Arguments
    ---------
        input_data: dict

    Returns
    -------
        dict
    """
    if registered_data:
        input_data["v3"] = registered_data["v3"]
    else:
        if not input_data["v3"] or _is_registered_v3(input_data["v3"]):
            input_data["v3"] = _get_unique_v3()
    return input_data


def _get_unique_v3():
    """
    Generate v3 and return it only if it is new

    Returns
    -------
        str
    """
    while True:
        generated = v3_gen.generates()
        if not _is_registered_v3(generated):
            return generated


def _prepare_data_to_register(document_data, registered):
    try:
        pkg = registered or models.Package()

        pkg.v3 = document_data["v3"]

        # outros tipos de ID
        pkg.v2 = document_data["v2"]
        pkg.aop_pid = document_data["aop_pid"]

        # dados que identificam o documento e que sempre estão presentes
        for item in document_data["issns"]:
            pkg.update_issns(item["type"], item["value"])
        pkg.pub_year = document_data["pub_year"]

        # dados que identificam o documento e não são obrigatórios
        for item in document_data["doi_with_lang"]:
            pkg.update_doi(item["lang"], item["value"])

        # authors and collab
        pkg.collab = document_data["collab"]
        for item in document_data["authors"]:
            pkg.update_authors(
                item["surname"], item["given_names"],
                item["orcid"],
                item["prefix"], item["suffix"],
            )

        # títulos do documento
        for item in document_data["article_titles"]:
            pkg.update_article_titles(
                item["lang"], item["text"])

        # dados complementares que identificam o documento
        pkg.volume = document_data["volume"]
        pkg.number = document_data["number"]
        pkg.suppl = document_data["suppl"]
        pkg.elocation_id = document_data["elocation_id"]
        pkg.fpage = document_data["fpage"]
        pkg.fpage_seq = document_data["fpage_seq"]
        pkg.lpage = document_data["lpage"]

        # quando o documento não tem metadados suficientes para identificar
        pkg.partial_body = _standardize_partial_body(
            document_data["partial_body"])

        pkg.xml = document_data["xml"]

        # dados de processamento / procedimentos
        pkg.extra = document_data["extra"]

        return pkg
    except Exception as e:
        raise exceptions.PrepareDataToSaveError(
            "Preparing data to save error: %s %s %s" %
            (type(e), e, document_data)
        )


def _register_document(pkg):
    try:
        saved = pkg.save()
        LOGGER.debug("Saved")
        return saved
    except Exception as e:
        raise exceptions.SavingError(
            "Saving error: %s %s %s" % (type(e), e, document_data)
        )


def _standardize_partial_body(body):
    """
    para ajudar a desambiguar textos que não tem metadados como:
    autores, títulos, doi, entre outros, como, por exemplo, erratas
    """
    body = body or ''
    body = " ".join([w for w in body.split() if w])
    return body[:500].upper()


def _fetch_records(**kwargs):
    try:
        return mongo_db.fetch_records(models.Package, **kwargs)
    except Exception as e:
        raise exceptions.FetchRecordsError(
            "Fetching records error: %s %s %s" % (type(e), e, kwargs)
        )


def _fetch_most_recent_document(**kwargs):
    try:
        registered = _fetch_records(**kwargs)
        return registered[0]
    except (IndexError, TypeError):
        # não está registrado
        return None
    except exceptions.FetchRecordsError as e:
        raise exceptions.FetchMostRecentRecordError(
            "Unable to _fetch_most_recent_document %s %s %s" %
            (type(e), e, kwargs)
        )


def _is_registered_v3(v3):
    try:
        return bool(_fetch_most_recent_document(**{"v3": v3}))
    except exceptions.FetchMostRecentRecordError:
        return False


def _is_registered_v2(v2):
    try:
        return bool(_fetch_most_recent_document(**{"v2": v2}))
    except exceptions.FetchMostRecentRecordError:
        return False
