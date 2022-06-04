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
from mongoengine.fields import GridFSError


DOI_CREATION_STATUS = ('auto_assigned', 'assigned_by_editor', 'UNK')
DOI_REGISTRATION_STATUS = ('registered', 'not_registered', 'UNK')
ISSN_TYPES = ('epub', 'ppub', 'l', 'scielo-id')


def utcnow():
    return datetime.utcnow()
    # return datetime.utcnow().isoformat().replace("T", " ") + "Z"


class TextAndLang(EmbeddedDocument):
    lang = StringField()
    text = StringField()

    def as_dict(self):
        return {"lang": self.lang, "text": self.text}

    def __str__(self):
        return self.to_json()


class DOI(EmbeddedDocument):
    lang = StringField()
    value = StringField()
    creation_status = StringField(choices=DOI_CREATION_STATUS)
    registration_status = StringField(choices=DOI_REGISTRATION_STATUS)

    def as_dict(self):
        return {
            "lang": self.lang, "value": self.value,
            "creation_status": self.creation_status,
            "registration_status": self.registration_status,
        }

    def __str__(self):
        return self.to_json()


class ISSN(EmbeddedDocument):
    value = StringField()
    type = StringField(choices=ISSN_TYPES)

    def as_dict(self):
        return {
            "type": self.type,
            "value": self.value,
        }

    def __str__(self):
        return self.to_json()


class Author(EmbeddedDocument):
    suffix = StringField()
    prefix = StringField()
    surname = StringField()
    given_names = StringField()
    orcid = StringField()

    def as_dict(self):
        return {
            "surname": self.surname,
            "given_names": self.given_names,
            "prefix": self.prefix,
            "suffix": self.suffix,
            "orcid": self.orcid,
        }

    def __str__(self):
        return self.to_json()


COMPOSE_VALUE_ATTRIBS = dict(
    issns=ISSN,
    doi_with_lang=DOI,
    authors=Author,
    article_titles=TextAndLang,
)


class Package(Document):

    # _id - generated automatically

    # pid v3
    v3 = StringField(max_length=23, required=True)

    # outros tipos de ID
    v2 = StringField()
    aop_pid = StringField()

    # dados que identificam o documento e que sempre estão presentes
    issns = EmbeddedDocumentListField(ISSN)
    pub_year = StringField()

    # dados que identificam o documento e não são obrigatórios
    doi_with_lang = EmbeddedDocumentListField(DOI)
    authors = EmbeddedDocumentListField(Author)
    collab = StringField()
    article_titles = EmbeddedDocumentListField(TextAndLang)
    surnames = StringField()

    # dados complementares que identificam o documento
    volume = StringField()
    number = StringField()
    suppl = StringField()
    elocation_id = StringField()
    fpage = StringField()
    fpage_seq = StringField()
    lpage = StringField()

    # quando o documento não tem metadados suficientes para identificar
    partial_body = StringField()

    # dados de processamento / procedimentos
    extra = DictField()

    # zipfile
    zip_file_path = FileField()

    # datas deste registro
    created = DateTimeField()
    updated = DateTimeField()

    meta = {
        'db_alias': 'scielo_core',
        'collection': 'id_provider',
        'indexes': [
            'v3',
            'v2',
            'aop_pid',
            'issns',
            'pub_year',
            'doi_with_lang',
            'authors',
            'collab',
            'surnames',
            'volume',
            'number',
            'suppl',
            'elocation_id',
            'fpage',
            'fpage_seq',
            'lpage',
            'partial_body'
        ]
    }

    @property
    def zip_file(self):
        return self.zip_file_path.read()

    @zip_file.setter
    def zip_file(self, file_path):
        try:
            self.zip_file_path.delete()
        except GridFSError:
            pass

        with open(file_path, 'rb') as fd:
            try:
                self.zip_file_path.put(fd, content_type='application/zip')
            except GridFSError:
                self.zip_file_path.replace(fd, content_type='application/zip')

    def as_dict(self, to_compare=False):
        """
        Retorna um dicionário com os atributos do documento
        """
        values = (
            self.v3,
            self.v2, self.aop_pid,
            self.issns, self.pub_year,
            self.doi_with_lang,
            self.authors, self.collab, self.surnames,
            self.article_titles,
            self.volume, self.number, self.suppl,
            self.elocation_id, self.fpage, self.fpage_seq, self.lpage,
            self.extra,
            self.partial_body,
        )
        labels = (
            "v3",
            "v2", "aop_pid",
            "issns", "pub_year",
            "doi_with_lang",
            "authors", "collab", "surnames",
            "article_titles",
            "volume", "number", "suppl",
            "elocation_id", "fpage", "fpage_seq", "lpage",
            "extra",
            "partial_body",
        )
        data = {}
        for label, value in zip(labels, values):
            try:
                data[label] = value.as_dict()
            except AttributeError:
                data[label] = value or ''
        if not to_compare:
            data['_id'] = str(self.id)
            data['created'] = str(self.created)
            data['updated'] = str(self.updated)
        return data

    def __str__(self):
        return self.to_json()

    def _get_surnames(self):
        return " ".join([author.surname for author in self.authors]).upper()

    def save(self, *args, **kwargs):
        self.surnames = self._get_surnames()
        if not self.created:
            self.created = utcnow()
        self.updated = utcnow()

        return super(Package, self).save(*args, **kwargs)

    def update_doi(self, lang, value, creation_status=None, registration_status=None):
        if not self.doi_with_lang:
            self.doi_with_lang = []
        if not all([lang, value]):
            return

        item = DOI()
        item.lang = lang
        item.value = value
        item.creation_status = creation_status or 'UNK'
        item.registration_status = registration_status or 'UNK'

        for i, registered in enumerate(self.doi_with_lang):
            if registered.lang == lang:
                self.doi_with_lang[i] = item
                return
        self.doi_with_lang.append(item)

    def update_authors(self, surname, given_names, orcid, prefix, suffix):
        if not self.authors:
            self.authors = []
        if not all([surname, given_names]):
            return

        item = Author()
        item.surname = surname
        item.given_names = given_names
        item.orcid = orcid or None
        item.prefix = prefix or None
        item.suffix = suffix or None

        for i, registered in enumerate(self.authors):
            if (registered.surname, registered.given_names) == (surname, given_names):
                self.authors[i] = item
                return
        self.authors.append(item)

    def update_issns(self, type, value):
        if not self.issns:
            self.issns = []
        if not all([type, value]):
            return

        item = ISSN()
        item.type = type
        item.value = value

        for i, registered in enumerate(self.issns):
            if registered.type == type:
                self.issns[i] = item
                return

        self.issns.append(item)

    def update_article_titles(self, lang, text):
        if not self.article_titles:
            self.article_titles = []
        if not all([lang, text]):
            return

        item = TextAndLang()
        item.lang = lang
        item.text = text

        for i, registered in enumerate(self.article_titles):
            if registered.lang == lang:
                self.article_titles[i] = item
                return

        self.article_titles.append(item)
