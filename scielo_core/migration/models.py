import logging
from datetime import datetime

from mongoengine import (
    Document,
    StringField,
    DateTimeField,
    BooleanField,
)


LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


MIGRATION_STATUS = ('GET_XML', 'TO_MIGRATE', 'MIGRATED')


def utcnow():
    return datetime.utcnow()
    # return datetime.utcnow().isoformat().replace("T", " ") + "Z"


class Migration(Document):
    # pid v3
    v3 = StringField()

    # outros tipos de ID
    v2 = StringField(max_length=23, required=True)
    aop_pid = StringField()

    file_path = StringField()

    xml_id = StringField()

    issn = StringField()
    year = StringField()
    order = StringField()

    v91 = StringField()
    v93 = StringField()

    status = StringField(choices=MIGRATION_STATUS)
    status_msg = StringField()
    is_aop = BooleanField()

    # datas deste registro
    created = DateTimeField()
    updated = DateTimeField()

    meta = {
        'db_alias': 'scielo_core',
        'collection': 'id_provider_migration',
        'indexes': [
            'v3',
            'v2',
            'aop_pid',
            'issn',
            'year',
            'order',
            'v91',
            'v93',
            'status',
            'xml_id',
        ]
    }

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = utcnow()
        self.updated = utcnow()

        return super(Migration, self).save(*args, **kwargs)
