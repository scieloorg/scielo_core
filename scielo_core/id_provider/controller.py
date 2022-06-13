import logging
from http import HTTPStatus

from scielo_core.basic import mongo_db
from scielo_core.id_provider import (
    models,
    exceptions,
    v3_gen,
    xml_sps,
)
from scielo_core.config import ID_PROVIDER_DB_URI


conn = mongo_db.mk_connection(ID_PROVIDER_DB_URI, 'scielo_core')

LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def get_xml(v3):
    try:
        doc = _fetch_most_recent_document(v3=v3)
        return doc.xml
    except exceptions.FetchMostRecentRecordError:
        return


def request_document_ids(pkg_file_path, user=None):
    LOGGER.debug(pkg_file_path)

    arguments = xml_sps.IdRequestArguments(pkg_file_path)

    request = _log_new_request(arguments.data, user)

    try:
        doc = _request_document_ids(**arguments.data)
        data = doc.as_dict()
        _log_request_update(request, data)
    except exceptions.DocumentIsUpdatedError:
        data = arguments.data
        _log_request_update(request, data)
    else:
        return doc.xml


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


def _request_document_ids(
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
    Obtém registro consultando com os dados do documento
    Cria o registro

    Returns
    -------
    dict
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
    input_data = document.attribs
    LOGGER.debug("Document %s" % input_data)

    # obtém o documento registrado
    try:
        registered_data = _get_registered_document_data(input_data)
    except exceptions.DocumentDoesNotExistError:
        LOGGER.debug("DocumentDoesNotExistError")
        registered_data = {}
    except exceptions.GetRegisteredDocumentError as e:
        raise exceptions.RequestDocumentIdError(e)

    data = _get_data_to_register(input_data, registered_data)
    if not data:
        LOGGER.debug("Document is already registered")
        raise exceptions.DocumentIsUpdatedError(
            "Document is already registered"
        )

    try:
        LOGGER.debug("Register document")
        return _register_document(data)
    except exceptions.SavingError as e:
        raise exceptions.RequestDocumentIdError(e)


def _get_data_to_register(input_data, registered_data):
    # novo, atualizado, pendente de atualização
    # novo: registrar
    # atualizado: finalizar
    # pendente de atualização: registrar
    if not registered_data:
        # novo
        input_data['xml'] = xml_sps.get_xml_from_zip_file(
            input_data['zip_file_path'])
        return input_data

    LOGGER.debug("Registered data: %s" % registered_data)
    if not need_to_update(input_data, registered_data):
        # documento já está registrado
        LOGGER.debug("Document is already updated %s" % input_data)
        return None

    # add pids
    data = _update_pids(input_data, registered_data)

    LOGGER.debug("Update xml: %s" % data)
    data["xml"] = xml_sps.update_ids(
        data["zip_file_path"],
        data["v3"],
        data["v2"],
        data["aop_pid"],
    )
    return data


def _update_pids(input_data, registered_data):
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
    registered_ids = _get_registered_document_ids(registered_data)
    LOGGER.debug("registered_ids: %s" % registered_ids)
    input_data = _add_pid_v3(input_data, registered_data)
    input_data = _add_aop_pid(input_data, registered_data)
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
        except KeyError:
            raise ValueError("issns must have type and value")

        document_data["pub_year"] = pub_year.upper()

        # dados que identificam o documento e não são obrigatórios
        try:
            document_data["doi_with_lang"] = [
                {"lang": item["lang"], "value": item["value"].upper()}
                for item in doi_with_lang
            ]
        except KeyError:
            raise ValueError("doi_with_lang must have lang and value")

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
        except KeyError:
            raise ValueError("authors must have surname and given_names")

        # títulos do documento
        try:
            document_data["article_titles"] = [
                {"lang": item["lang"], "text": item["text"].upper()}
                for item in article_titles
            ]
        except KeyError:
            raise ValueError("article_titles must have lang and text")

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


def need_to_update(input_data, registered_data):
    for k in ("aop_pid", "v2", "v3"):
        if (input_data.get(k) or '') != (registered_data.get(k) or ''):
            return True
    return False


def _get_registered_document_data(document_attribs):
    """
    Obtém registro consultando com dados do documento:
        - com pid v2
        - sem pid v2
        - versão aop

    Arguments
    ---------
        document: Document

    Returns
    -------
        Package

    """
    # busca o documento com dados do fascículo + pid v2
    try:
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

    except (
            exceptions.QueryingDocumentInIssueError,
            exceptions.QueryingDocumentAsAOPError,
            exceptions.FetchMostRecentRecordError,
            ) as e:
        raise exceptions.GetRegisteredDocumentError(e)

    try:
        doc = registered.as_dict()
    except AttributeError:
        raise exceptions.DocumentDoesNotExistError(
            "Document does not exist %s" % document_attribs)

    try:
        return _fetch_most_recent_document(v3=doc["v3"]).as_dict()
    except KeyError as e:
        raise exceptions.GetRegisteredDocumentError(e)
    except exceptions.FetchMostRecentRecordError as e:
        raise exceptions.GetRegisteredDocumentError(e)


def _get_registered_document_ids(registered_data):
    """
    Obtém os IDs do documento registrado

    Arguments
    ---------
        registered_data: dict

    Returns
    -------
        None ou dict

    """
    if registered_data:
        _data = {}
        for k in ("v2", "v3", "aop_pid"):
            try:
                _data[k] = registered_data[k]
            except KeyError:
                pass
        return _data


def _get_query_parameters(document_attribs, with_v2=False, aop_version=False):
    """
    Obtém os parâmetros para buscar um documento

    Arguments
    ---------
        document: Document
        with_v2: bool
        aop_version: bool
    """
    params = {}
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

    if document_attribs["authors"]:
        params["surnames"] = " ".join([
            author["surname"] for author in document_attribs["authors"]
        ])

    for k in ("doi_with_lang", "authors", "collab", "article_titles"):
        if document_attribs[k]:
            break
        # nenhum destes, então procurar pelo início do body
        params["partial_body"] = document_attribs["partial_body"]

    if aop_version:
        params.pop("pub_year")
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
    return params


def _get_document_published_in_an_issue(document_attribs, with_v2=False):
    """
    Busca documento com os dados do artigo + issue

    Arguments
    ---------
        document_attribs: dict
        with_v2: bool
            usa ou não o v2 na consulta
    """
    params = _get_query_parameters(document_attribs, with_v2=with_v2)
    try:
        return _fetch_most_recent_document(**params)
    except exceptions.FetchMostRecentRecordError as e:
        raise exceptions.QueryingDocumentInIssueError(
            f"Querying document in an issue error: {e}"
        )


def _get_document_published_as_aop(document_attribs):
    """
    Busca pela versão aop, ou seja, busca pelos dados do artigo:
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
    params = _get_query_parameters(document_attribs, aop_version=True)
    try:
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
    if (not registered_data.get("volume") and
            not registered_data.get("number") and
            not registered_data.get("suppl")):
        input_data["aop_pid"] = registered_data["v2"]
    return input_data


##############################################


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


def _register_document(document_data):
    try:
        pkg = models.Package()

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
            (kwargs, type(e), e)
        )


def _is_registered_v3(v3):
    try:
        return bool(_fetch_most_recent_document(**{"v3": v3}))
    except exceptions.FetchMostRecentRecordError:
        return False
