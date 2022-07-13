import logging

from lxml import etree

from packtools.sps.models.article_ids import ArticleIds
from packtools.sps.models.article_doi_with_lang import DoiWithLang
from packtools.sps.models.front_journal_meta import ISSN
from packtools.sps.models.front_articlemeta_issue import ArticleMetaIssue
from packtools.sps.models.article_authors import Authors
from packtools.sps.models.article_titles import ArticleTitles
from packtools.sps.models.body import Body
from packtools.sps.models.dates import ArticleDates

from scielo_core.basic import xml_sps_zip_file
from scielo_core.basic import exceptions


LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


get_xml_content = xml_sps_zip_file.get_xml_content


def split_processing_instruction_doctype_declaration_and_xml(xml_content):
    xml_content = xml_content.strip()

    p = xml_content.rfind("</")
    endtag = xml_content[p:]
    starttag = endtag.replace("/", "").replace(">", "").strip()

    p = xml_content.find(starttag)
    return xml_content[:p], xml_content[p:].strip()


def get_xml_tree(xml_content):
    try:
        # return etree.fromstring(xml_content)
        pref, xml = split_processing_instruction_doctype_declaration_and_xml(
            xml_content
        )
        return etree.fromstring(xml)

    except etree.XMLSyntaxError as e:
        raise exceptions.InvalidXMLError(e)


def update_ids(xml_content, v3, v2, aop_pid):
    xmltree = get_xml_tree(xml_content)

    # update IDs
    article_ids = ArticleIds(xmltree)
    article_ids.v3 = v3
    article_ids.v2 = v2
    if aop_pid:
        article_ids.aop_pid = aop_pid

    # update XML
    return etree.tostring(
        article_ids._xmltree, encoding="utf-8").decode("utf-8")


class IdRequestArguments:

    def __init__(self, zip_file_path):
        self.xml_content = xml_sps_zip_file.get_xml_content(zip_file_path)
        self.xmltree = get_xml_tree(self.xml_content)
        self.zip_file_path = zip_file_path

    @property
    def data(self):
        _data = self.article_ids
        _data.update(self.article_doi_with_lang)
        _data.update(self.issns)
        _data.update(self.article_in_issue)
        _data.update(self.authors)
        _data.update(self.article_titles)
        _data.update(self.partial_body)
        _data['xml'] = self.xml_content
        return _data

    @property
    def article_ids(self):
        article_ids = ArticleIds(self.xmltree)
        data = article_ids.data
        return {
            k: data.get(k)
            for k in ("v3", "v2", "aop_pid")
        }

    @property
    def article_doi_with_lang(self):
        doi_with_lang = DoiWithLang(self.xmltree)
        return {"doi_with_lang": doi_with_lang.data}

    @property
    def issns(self):
        issns = ISSN(self.xmltree)
        return {"issns": issns.data}

    @property
    def article_in_issue(self):
        _data = {
            k: '' for k in (
                "volume", "number", "suppl",
                "fpage", "fpage_seq", "lpage",
                "elocation_id",
                "pub_year",
            )
        }
        article_in_issue = ArticleMetaIssue(self.xmltree)
        _data.update(article_in_issue.data)
        if not _data.get("pub_year"):
            _data["pub_year"] = ArticleDates(self.xmltree).article_date["year"]
        return _data

    @property
    def authors(self):
        authors = Authors(self.xmltree)
        data = {
            "authors": authors.contribs,
            "collab": authors.collab or '',
        }
        return data

    @property
    def article_titles(self):
        article_titles = ArticleTitles(self.xmltree)
        return {"article_titles": article_titles.data}

    @property
    def partial_body(self):
        try:
            body = Body(self.xmltree)
            for text in body.main_body_texts:
                if text:
                    return {"partial_body": text}
        except AttributeError:
            return {"partial_body": ''}
        return {"partial_body": ''}
