from datetime import datetime

from mongoengine import (
    EmbeddedDocument,
    EmbeddedDocumentListField,
    Document,
    StringField,
    DateTimeField,
    DictField,
    FileField,
)

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
        'collection': 'migration',
        'indexes': [
            'v3',
            'v2',
            'aop_pid',
            'issn',
            'year',
            'order',
            'v91',
            'v92',
        ]
    }

    @property
    def zip_file(self):
        return self.xml_file_path.read()

    @zip_file.setter
    def zip_file(self, file_path):
        with open(file_path, 'rb') as fd:
            self.xml_file_path.put(fd, content_type='application/zip')

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = utcnow()
        self.updated = utcnow()

        return super(Migration, self).save(*args, **kwargs)
