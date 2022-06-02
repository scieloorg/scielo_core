import json

from scielo_core.migration import tasks


def register_migration(docs_jsonl_file_path, issns_file_path):
    with open(docs_jsonl_file_path, "r") as fp:
        issns = set()
        for row in fp.readlines():
            data = json.loads(row.strip())
            issns.add(data["issn"])
            tasks.register_migration(data)

    with open(issns_file_path, "w") as fp:
        fp.write("\n".join(issns))


def harvest_journal_xmls(issns_file_path):
    with open(issns_file_path) as fp:
        for issn in fp.readlines():
            issn = issn.strip()
            tasks.harvest_journal_xmls(issn)


def migrate_journal_xmls(issns_file_path):
    with open(issns_file_path) as fp:
        for issn in fp.readlines():
            issn = issn.strip()
            tasks.migrate_journal_xmls(issn)
