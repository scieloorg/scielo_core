import logging

from scielo_core.basic import mongo_db, xml_sps_zip_file
from scielo_core.id_provider import (
    models,
    exceptions,
    v3_gen,
    xml_sps,
)
from scielo_core.config import DATABASE_CONNECT_URL


conn = mongo_db.mk_connection(DATABASE_CONNECT_URL, 'scielo_core')

LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


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
        ):
    """
    Obtém registro consultando com os dados do documento
    Cria o registro
    Retorna dicionário cujas chaves são:
        input, registered, created, exception
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
    document_attribs = document.attribs
    LOGGER.debug("Document %s" % document_attribs)

    # obtém o documento registrado
    registered = _get_registered_document(document_attribs)
    try:
        registered_data = registered.as_dict()
    except AttributeError:
        registered_data = {}
    LOGGER.debug("Registered data: %s" % registered_data)

    # extrai dados do documento registrado
    recovered_ids = _get_registered_document_ids(registered_data)
    LOGGER.debug("recovered_ids: %s" % recovered_ids)

    # acrescenta os dados registrados aos dados de entrada
    data_to_register = _add_pids_to_input_data(
        document_attribs, recovered_ids, registered_data
    )
    LOGGER.debug("data_to_register: %s" % data_to_register)

    if not data_to_register:
        # documento já está registrado
        return registered_data

    data_to_register["zip_file_path"] = xml_sps.update_ids(
        data_to_register["zip_file_path"],
        data_to_register["v3"],
        data_to_register["v2"],
        data_to_register["aop_pid"],
    )

    LOGGER.debug("data_to_register updated: %s" % data_to_register)
    return _register_document(data_to_register).as_dict()


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


def is_diff(input_data, registered_data):
    for k in ("aop_pid", "v2", "v3"):
        if (input_data.get(k) or '') != (registered_data.get(k) or ''):
            return True
    return False


def _get_registered_document(document_attribs):
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

    if registered:
        LOGGER.debug("Find the most recent version")
        return _fetch_most_recent_document(v3=registered.as_dict()["v3"])


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
    except exceptions.FetchRecordsError as e:
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
    except exceptions.FetchRecordsError as e:
        raise exceptions.QueryingDocumentAsAOPError(
            f"Querying document as aop error: {e}"
        )


def _add_pids_to_input_data(document_data, recovered_ids, registered_data):
    """
    Atualiza document_data com IDs

    Arguments
    ---------
        document_data: dict
        recovered_ids: dict
        registered_data: dict

    Returns
    -------
        dict
    """
    if not recovered_ids:
        # retorna os dados do documento enviado
        return _add_pid_v3(document_data)

    if (not registered_data.get("volume") and
            not registered_data.get("number") and
            not registered_data.get("suppl")):
        recovered_ids["aop_pid"] = recovered_ids.pop("v2")

    document_data.update(recovered_ids)
    if not is_diff(document_data, recovered_ids):
        return None

    return document_data


##############################################


def _add_pid_v3(document_attribs):
    """
    Garante que document_attribs tenha um v3 inédito

    Arguments
    ---------
        document_attribs: dict

    Returns
    -------
        dict
    """
    if not document_attribs["v3"] or _is_registered_v3(document_attribs["v3"]):
        document_attribs["v3"] = _get_unique_v3()
    return document_attribs


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


def _register_document(document_attribs):
    try:
        pkg = models.Package()

        pkg.v3 = document_attribs["v3"]

        # outros tipos de ID
        pkg.v2 = document_attribs["v2"]
        pkg.aop_pid = document_attribs["aop_pid"]

        # dados que identificam o documento e que sempre estão presentes
        for item in document_attribs["issns"]:
            pkg.update_issns(item["type"], item["value"])
        pkg.pub_year = document_attribs["pub_year"]

        # dados que identificam o documento e não são obrigatórios
        for item in document_attribs["doi_with_lang"]:
            pkg.update_doi(item["lang"], item["value"])

        # authors and collab
        pkg.collab = document_attribs["collab"]
        for item in document_attribs["authors"]:
            pkg.update_authors(
                item["surname"], item["given_names"],
                item["orcid"],
                item["prefix"], item["suffix"],
            )

        # títulos do documento
        for item in document_attribs["article_titles"]:
            pkg.update_article_titles(
                item["lang"], item["text"])

        # dados complementares que identificam o documento
        pkg.volume = document_attribs["volume"]
        pkg.number = document_attribs["number"]
        pkg.suppl = document_attribs["suppl"]
        pkg.elocation_id = document_attribs["elocation_id"]
        pkg.fpage = document_attribs["fpage"]
        pkg.fpage_seq = document_attribs["fpage_seq"]
        pkg.lpage = document_attribs["lpage"]

        # quando o documento não tem metadados suficientes para identificar
        pkg.partial_body = _standardize_partial_body(
            document_attribs["partial_body"])

        pkg.zip_file = document_attribs["zip_file_path"]

        # dados de processamento / procedimentos
        pkg.extra = document_attribs["extra"]

        return pkg.save()
    except Exception as e:
        raise exceptions.SavingError(
            "Saving error: %s %s %s" % (type(e), e, document_attribs)
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
    registered = _fetch_records(**kwargs)

    try:
        return registered[0]
    except (IndexError, TypeError):
        # não está registrado
        return None


def _is_registered_v3(v3):
    return bool(_fetch_most_recent_document(**{"v3": v3}))
