import logging

from opac_schema.v1.models import Article

from scielo_core.basic import mongo_db
from scielo_core.migration import models

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
        LOGGER.debug("Not found %s" % v2)
        return None


def save_migration(v2, aop_pid, file_path, issn, year, order, v91, v93, skip_update=False):
    try:
        migration = _fetch_migration_records(**{"v2": v2})[0]
        if skip_update:
            LOGGER.debug("Skip update %s" % v2)
            return migration
    except IndexError:
        migration = models.Migration()
        migration.v2 = v2
    migration.status = 'GET_XML'

    # outros tipos de ID
    migration.aop_pid = aop_pid

    migration.file_path = file_path
    migration.issn = issn
    migration.year = year
    migration.order = order
    migration.v91 = v91
    migration.v93 = v93
    migration.save()
    return migration


def update_migration(v2, v3, zip_file):
    try:
        migration = _fetch_migration_records(**{"v2": v2})[0]
    except IndexError:
        raise FetchMigrationError("Unable to find migration: %s" % v2)

    migration.v3 = v3
    migration.zip_file = zip_file
    if zip_file:
        migration.status = "TO_MIGRATE"
    migration.save()
    return migration


def get_pids(issn, order_by=None, status=None):
    page = 0
    while True:
        page += 1
        try:
            kwargs = {"issn": issn, "page": page}
            if status:
                kwargs['status'] = status
            if order_by:
                kwargs['order_by'] = order_by
            records = _fetch_migration_records(**kwargs)
        except Exception as e:
            LOGGER.exception(
                "Unable to fetch records %s %s %s %s" %
                (issn, page, type(e), e)
            )
        else:
            for item in records:
                yield item.v2
            else:
                break
