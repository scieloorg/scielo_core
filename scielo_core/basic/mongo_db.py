"""
Module with generic functions for any schema
"""
from datetime import datetime

from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
)
from mongoengine.errors import NotUniqueError
from mongoengine import (
    connect,
    Q,
)

from scielo_core.basic import exceptions


def mk_connection(host):
    try:
        _db_connect_by_uri(host)
    except Exception as e:
        raise exceptions.DBConnectError(
            {"exception": type(e), "msg": str(e)}
        )


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def _db_connect_by_uri(uri):
    """
    mongodb://{login}:{password}@{host}:{port}/{database}
    """
    conn = connect(host=uri, maxPoolSize=None)
    print("%s connected" % uri)
    return conn


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def _db_connect(host, port, schema, login, password, **extra_dejson):
    uri = "mongodb://{creds}{host}{port}/{database}".format(
        creds="{}:{}@".format(login, password) if login else "",
        host=host,
        port="" if port is None else ":{}".format(port),
        database=schema,
    )

    return connect(host=uri, **extra_dejson)


def create_record(Model):
    try:
        return Model()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(
            {"exception": type(e), "msg": str(e)}
        )


def save_record(record):
    if not hasattr(record, 'created'):
        record.created = None
    try:
        record.updated = datetime.utcnow()
        if not record.created:
            record.created = record.updated
        record.save()
    except NotUniqueError as e:
        raise exceptions.DBSaveNotUniqueError(e)
    else:
        return record


def update_record_with_data(doc, data, COMPOSE_VALUE_ATTRIBS):
    # doc._id
    for doc_attr_name, doc_attr_value in data.items():
        _doc_attr_value = doc_attr_value

        CLASS = COMPOSE_VALUE_ATTRIBS.get(doc_attr_name)
        if CLASS:
            # atributo multivalorado e composto (lista de "dicionários")
            _doc_attr_value = []
            for item in doc_attr_value:
                obj = CLASS()
                for obj_attr, obj_val in item.items():
                    setattr(obj, obj_attr, obj_val)
                _doc_attr_value.append(obj)
        setattr(doc, doc_attr_name, _doc_attr_value)
    return doc


def select_attributes(attributes, selected_attrib_names):
    """
    Select `attributes` from the list `selected_attrib_names`

    Parameters
    ----------
    attributes: dict
    selected_attrib_names: list

    Returns
    -------
    list
    """
    selection = []
    for k in selected_attrib_names:
        try:
            selection.extend(attributes[k])
        except KeyError:
            continue
    return selection


def multi_value_attribs_to_single_value_attribs(MULTI_VALUE_ATTRIBS, **kwargs):
    """
    Convert `{"attr1": ["a", "b"], "attr2": ["x", "y"]}` to
            `{"attr1": [{"attr1": "a"}, {"attr1": "b"}],
              "attr2": [{"attr2": "x"}, {"attr2": "y"}]}`

    """
    args = {}
    for k, v in kwargs.items():
        if k in MULTI_VALUE_ATTRIBS:
            args[k] = [
                {k: item} for item in v
            ]
        else:
            args[k] = [{k: v}]
    return args


def queryset_with_multivalues(attribute_name, values):
    """
    Obtém QuerySet
    """
    Qs = None
    for value in values:
        kwargs = {
            f"{attribute_name}": value
        }
        if Qs:
            Qs |= Q(**kwargs)
        else:
            Qs = Q(**kwargs)
    return Qs


def query_set_with_or_operator(arguments):
    """
    Obtém QuerySet
    """
    Qs = None
    for _kwargs in arguments:
        if Qs:
            Qs |= Q(**_kwargs)
        else:
            Qs = Q(**_kwargs)
    return Qs


def query_set_with_and_operator(arguments):
    """
    Obtém QuerySet
    """
    Qs = None
    for _kwargs in arguments:
        if Qs:
            Qs &= Q(**_kwargs)
        else:
            Qs = Q(**_kwargs)
    return Qs


def fetch_records(Model, **kwargs):
    try:
        order_by = kwargs.pop("order_by")
    except KeyError:
        order_by = '-updated'

    try:
        items_per_page = kwargs.pop("items_per_page")
    except KeyError:
        items_per_page = 50

    try:
        page = kwargs.pop("page")
    except KeyError:
        page = 1

    try:
        qs = kwargs.pop("qs")
    except KeyError:
        qs = None

    skip = ((page - 1) * items_per_page)
    limit = items_per_page

    if qs and kwargs:
        return Model.objects(
            qs, **kwargs
        ).order_by(order_by).skip(skip).limit(limit)

    if qs:
        return Model.objects(
            qs
        ).order_by(order_by).skip(skip).limit(limit)

    return Model.objects(
            **kwargs
        ).order_by(order_by).skip(skip).limit(limit)


def _get_EmbeddedDocumentListField_query_params(
        items, field_name, field_attribute):
    if not items:
        return {}
    params = {}
    # example: issns__value
    param_name = f"{field_name}__{field_attribute}"
    values = [item[field_attribute].upper() for item in items]
    if len(values) == 1:
        params[param_name] = values[0]
    else:
        kwargs = [
            {param_name: value}
            for value in values
        ]
        params["qs"] = query_set_with_or_operator(kwargs)
    return params
