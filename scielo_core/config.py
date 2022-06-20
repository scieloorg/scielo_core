import os


# mongodb://my_user:my_password@127.0.0.1:27017/my_db
SCIELO_CORE_ID_PROVIDER_DB_URI = (
    os.environ.get("SCIELO_CORE_ID_PROVIDER_DB_URI") or
    "mongodb://127.0.0.1:27017/my_id_provider"
)
SCIELO_CORE_WEBSITE_DB_URI = (
    os.environ.get("SCIELO_CORE_WEBSITE_DB_URI") or
    "mongodb://127.0.0.1:27017/my_website"
)

SCIELO_CORE_ID_PROVIDER_CELERY_BROKER_URL = os.environ.get(
    "SCIELO_CORE_ID_PROVIDER_CELERY_BROKER_URL", 'amqp://guest@0.0.0.0:5672//')
SCIELO_CORE_ID_PROVIDER_CELERY_RESULT_BACKEND_URL = os.environ.get(
    "SCIELO_CORE_ID_PROVIDER_CELERY_RESULT_BACKEND_URL", 'rpc://')

SCIELO_CORE_MIGRATION_CELERY_BROKER_URL = os.environ.get(
    "SCIELO_CORE_MIGRATION_CELERY_BROKER_URL", 'amqp://guest@0.0.0.0:5672//')
SCIELO_CORE_MIGRATION_CELERY_RESULT_BACKEND_URL = os.environ.get(
    "SCIELO_CORE_MIGRATION_CELERY_RESULT_BACKEND_URL", 'rpc://')

SCIELO_CORE_CONCURRENT_PROCESSING = os.environ.get(
    'SCIELO_CORE_CONCURRENT_PROCESSING') or True


SCIELO_CORE_REGISTER_MIGRATION_QUEUE = (
    os.environ.get("SCIELO_CORE_REGISTER_MIGRATION_QUEUE") or 'migr_default')
SCIELO_CORE_HARVEST_XMLS_QUEUE = (
    os.environ.get("SCIELO_CORE_HARVEST_XMLS_QUEUE") or 'migr_high_priority')
SCIELO_CORE_MIGRATE_XMLS_QUEUE = (
    os.environ.get("SCIELO_CORE_MIGRATE_XMLS_QUEUE") or 'migr_high_priority')
SCIELO_CORE_UNDO_ID_REQUEST_QUEUE = (
    os.environ.get("SCIELO_CORE_UNDO_ID_REQUEST_QUEUE") or 'migr_high_priority')

SCIELO_CORE_MIGRATION_LOGFILE = os.environ.get(
    'SCIELO_CORE_MIGRATION_LOGFILE') or 'migration.log'
SCIELO_CORE_ID_PROVIDER_LOGFILE = os.environ.get(
    'SCIELO_CORE_ID_PROVIDER_LOGFILE') or 'id_provider.log'


def run_concurrently():
    return (
        SCIELO_CORE_CONCURRENT_PROCESSING and
        SCIELO_CORE_ID_PROVIDER_CELERY_BROKER_URL and
        SCIELO_CORE_ID_PROVIDER_CELERY_RESULT_BACKEND_URL
    )


def get_article_meta_uri(pid, col):
    return f'https://articlemeta.scielo.org/api/v1/article/?collection={col}&code={pid}&format=xmlrsps'
