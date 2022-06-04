import logging
from datetime import datetime

from mongoengine import (
    Document,
    StringField,
    DateTimeField,
    FileField,
)
from mongoengine.fields import GridFSError


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

    xml_file_path = FileField()

    issn = StringField()
    year = StringField()
    order = StringField()

    v91 = StringField()
    v93 = StringField()

    status = StringField(choices=MIGRATION_STATUS)

    # datas deste registro
    created = DateTimeField()
    updated = DateTimeField()

    meta = {
        'db_alias': 'scielo_core',
        'collection': 'migration',
        'indexes': [
            'v3',
            'v2',
            'aop_pid',
            'issn',
            'year',
            'order',
            'v91',
            'v93',
        ]
    }

    @property
    def zip_file(self):
        return self.xml_file_path.read()

    @zip_file.setter
    def zip_file(self, file_path):
        try:
            self.xml_file_path.delete()
        except GridFSError as e:
            LOGGER.debug("Unable to delete %s %s" % (self.xml_file_path, e))

        with open(file_path, 'rb') as fd:
            try:
                LOGGER.debug("Try to put %s %s" % (self.xml_file_path, e))
                self.xml_file_path.put(fd, content_type='application/zip')
            except GridFSError:
                LOGGER.debug("Try to replace %s %s" % (self.xml_file_path, e))
                self.xml_file_path.replace(fd, content_type='application/zip')

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = utcnow()
        self.updated = utcnow()

        return super(Migration, self).save(*args, **kwargs)
