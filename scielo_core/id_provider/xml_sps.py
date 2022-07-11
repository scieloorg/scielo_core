import logging

from lxml import etree
from packtools.sps.models.article_ids import ArticleIds
from packtools.sps.models.article_doi_with_lang import DoiWithLang
from packtools.sps.models.front_journal_meta import ISSN
from packtools.sps.models.front_articlemeta_issue import ArticleMetaIssue
from packtools.sps.models.article_authors import Authors
from packtools.sps.models.article_titles import ArticleTitles
from packtools.sps.models.body import Body

from scielo_core.basic import xml_sps_zip_file
from scielo_core.basic import exceptions


LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


get_xml_content = xml_sps_zip_file.get_xml_content


def get_xml_tree(xml_content):
    try:
        return etree.fromstring(xml_content)
    except etree.XMLSyntaxError as e:
        raise exceptions.InvalidXMLError(e)


def update_ids(xml_content, v3, v2, aop_pid):
    xmltree = get_xml_tree(xml_content)

    # update IDs
    article_ids = ArticleIds(xmltree)
    article_ids.v3 = v3
    article_ids.v2 = v2
    article_ids.aop_pid = aop_pid

    # update XML
    return etree.tostring(
        article_ids._xmltree, encoding="utf-8").decode("utf-8")


class IdRequestArguments:

    def __init__(self, zip_file_path):
        self.xmltree = get_xml_tree(xml_sps_zip_file.get_xml_content(zip_file_path))
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
        _data['zip_file_path'] = self.zip_file_path
        return _data

    @property
    def article_ids(self):
        article_ids = ArticleIds(self.xmltree)
        data = article_ids.data
        return {
            "v2": data.get("v2"),
            "v3": data.get("v3"),
            "aop_pid": data.get("aop_pid"),
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
