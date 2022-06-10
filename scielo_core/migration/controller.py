import logging

from opac_schema.v1.models import Article

from scielo_core.basic import mongo_db
from scielo_core.basic import exceptions
from scielo_core.migration import models
from scielo_core.id_provider import xml_sps

from scielo_core.config import DATABASE_CONNECT_URL
from scielo_core.config import WEBSITE_DB_URI

mongo_db.mk_connection(WEBSITE_DB_URI)
mongo_db.mk_connection(DATABASE_CONNECT_URL, 'scielo_core')


LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


class FetchArticleError(Exception):
    ...


class FetchMigrationError(Exception):
    ...


class InvalidXMLError(Exception):
    ...


class SaveMigrationError(Exception):
    ...


class GetPIDsError(Exception):
    ...


def _save_migration(migration):
    try:
        migration.save()
    except Exception as e:
        raise SaveMigrationError(e)


def update_status(migration, status, status_msg):
    migration.status = status
    migration.status_msg = status_msg
    _save_migration(migration)


def get_xml(v2):
    records = _fetch_migration_records(v2=v2)
    return records[0].xml.decode("utf-8")


def get_article(pid):
    try:
        kwargs = {"pid": pid}
        return _fetch_article_records(**kwargs)[0]
    except IndexError:
        return None


def _fetch_article_records(**kwargs):
    try:
        return mongo_db.fetch_records(Article, **kwargs)
    except Exception as e:
        LOGGER.exception(
            "Fetching Article %s: %s %s" % (kwargs, type(e), str(e)))
        raise FetchArticleError(e)


def _fetch_migration_records(**kwargs):
    try:
        return mongo_db.fetch_records(models.Migration, **kwargs)
    except Exception as e:
        LOGGER.exception(
            "Fetching Migration %s: %s %s" % (kwargs, type(e), str(e)))
        raise FetchMigrationError(e)


def get_migration(v2):
    try:
        return _fetch_migration_records(**{"v2": v2})[0]
    except IndexError:
        raise FetchMigrationError("Migration record %s not found" % v2)


def create_migration(v2, aop_pid, file_path, issn, year, order, v91, v93,
                     is_aop, skip_update=False):
    try:
        migration = get_migration(v2)
    except FetchMigrationError:
        migration = models.Migration()
        migration.v2 = v2
    else:
        if skip_update:
            LOGGER.debug("Skip update %s" % v2)
            return migration

    migration.status = 'GET_XML'

    # outros tipos de ID
    migration.aop_pid = aop_pid

    migration.file_path = file_path
    migration.issn = issn
    migration.year = year
    migration.order = order
    migration.v91 = v91
    migration.v93 = v93
    migration.is_aop = is_aop
    _save_migration(migration)
    return migration


def add_xml_and_v3(v2, v3, xml_id):
    try:
        migration = _fetch_migration_records(**{"v2": v2})[0]
    except IndexError:
        raise FetchMigrationError("Unable to find migration: %s" % v2)

    if v3:
        migration.v3 = v3
    try:
        migration.xml_id = xml_id
        migration.status = "TO_MIGRATE"
        migration.status_msg = ""
    except exceptions.InvalidXMLError as e:
        migration.status_msg = str(e)

    _save_migration(migration)
    return migration


def get_pids(issn, status, is_aop=None, order_by=None):
    page = 0

    kwargs = {"issn": issn}
    if status:
        kwargs['status'] = status
    if order_by:
        kwargs['order_by'] = order_by
    if is_aop in (True, False):
        kwargs['is_aop'] = is_aop

    while True:
        page += 1
        try:
            kwargs['page'] = page
            records = _fetch_migration_records(**kwargs)
        except Exception as e:
            raise GetPIDsError(
                "Unable to fetch records %s %s %s %s" %
                (issn, page, type(e), e)
            )
        else:
            for item in records:
                yield item.v2
            else:
                break
