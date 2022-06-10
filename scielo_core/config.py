import os


# mongodb://my_user:my_password@127.0.0.1:27017/my_db
ID_PROVIDER_DB_URI = (
    os.environ.get("ID_PROVIDER_DB_URI") or
    "mongodb://127.0.0.1:27017/my_id_provider"
)
WEBSITE_DB_URI = (
    os.environ.get("WEBSITE_DB_URI") or
    "mongodb://127.0.0.1:27017/my_website"
)

CELERY_BROKER_URL = os.environ.get(
    "CELERY_BROKER_URL", 'amqp://guest@0.0.0.0:5672//')
CELERY_RESULT_BACKEND_URL = os.environ.get(
    "CELERY_RESULT_BACKEND_URL", 'rpc://')

CONCURRENT_PROCESSING = os.environ.get('CONCURRENT_PROCESSING') or True


EXAMPLE_QUEUE = os.environ.get("EXAMPLE_QUEUE") or 'migr_low_priority'
REGISTER_MIGRATION_QUEUE = (
    os.environ.get("REGISTER_MIGRATION_QUEUE") or 'migr_default')
HARVEST_XMLS_QUEUE = (
    os.environ.get("HARVEST_XMLS_QUEUE") or 'migr_high_priority')
MIGRATE_XMLS_QUEUE = (
    os.environ.get("MIGRATE_XMLS_QUEUE") or 'migr_high_priority')

MIGRATION_LOGFILE = os.environ.get('MIGRATION_LOGFILE') or 'migration.log'


def run_concurrently():
    return (
        CONCURRENT_PROCESSING and
        CELERY_RESULT_BACKEND_URL and
        CELERY_BROKER_URL
    )


def get_article_meta_uri(pid, col):
	return f'https://articlemeta.scielo.org/api/v1/article/?collection={col}&code={pid}&format=xmlrsps'