import argparse
import json
import logging
import sys

from scielo_core.migration import tasks

LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def register_migration(docs_jsonl_file_path, issns_file_path, skip_update=False):
    print(skip_update)
    print(type(skip_update))
    with open(docs_jsonl_file_path, "r") as fp:
        issns = set()
        for row in fp.readlines():
            LOGGER.info(row)
            data = json.loads(row.strip())
            issns.add(data["issn"])
            resp = tasks.register_migration(data, skip_update, True)
            LOGGER.info(resp)

    with open(issns_file_path, "w") as fp:
        fp.write("\n".join(issns))


def harvest_journals_xmls(issns_file_path):
    with open(issns_file_path) as fp:
        for issn in fp.readlines():
            issn = issn.strip()
            tasks.harvest_journal_xmls(issn)


def migrate_journals_xmls(issns_file_path):
    with open(issns_file_path) as fp:
        for issn in fp.readlines():
            issn = issn.strip()
            tasks.migrate_journal_xmls(issn)


def cli(argv=None):
    if argv is None:
        argv = sys.argv
    parser = argparse.ArgumentParser(
        description="SciELO ID Provider Migration command line utility.",
    )
    # parser.add_argument("-v", "--version", action="version", version=VERSION)
    parser.add_argument("--loglevel", default="")

    subparsers = parser.add_subparsers(
        title="Commands", metavar="", dest="command",
    )

    parser_register_migration = subparsers.add_parser(
        "register_migration",
        help="Register data from artigo.mst to control the migration",
        description="Register data from artigo.mst to control the migration",
    )
    parser_register_migration.add_argument(
        "docs_jsonl_file_path",
        help="jsonl file path which contains some metadata from artigo.mst",
    )
    parser_register_migration.add_argument(
        "issns_file_path",
        help="file path to save an ISSN list",
    )
    parser_register_migration.add_argument(
        "--skip_update",
        type=bool,
        help="Skip update",
    )

    parser_harvest_journals_xmls = subparsers.add_parser(
        "harvest_journals_xmls",
        help="Harvest the XML from the new website",
        description="Harvest the XML from the new website",
    )
    parser_harvest_journals_xmls.add_argument(
        "issns_file_path",
        help="file path to save an ISSN list",
    )

    parser_migrate_journals_xmls = subparsers.add_parser(
        "migrate_journals_xmls",
        help="Harvest the XML from the new website",
        description="Harvest the XML from the new website",
    )
    parser_migrate_journals_xmls.add_argument(
        "issns_file_path",
        help="file path to save an ISSN list",
    )

    args = parser.parse_args()
    # todas as mensagens serão omitidas se level > 50
    logging.basicConfig(
        level=getattr(logging, args.loglevel.upper(), 999), format=LOGGER_FMT
    )
    if args.command == "register_migration":
        register_migration(
            args.docs_jsonl_file_path, args.issns_file_path, args.skip_update)
    elif args.command == "harvest_journals_xmls":
        harvest_journals_xmls(args.issns_file_path)
    elif args.command == "migrate_journals_xmls":
        migrate_journals_xmls(args.issns_file_path)
    else:
        parser.print_help()


def main():
    try:
        sys.exit(cli())
    except KeyboardInterrupt:
        LOGGER.info("Got a Ctrl+C. Terminating the program.")
        # É convencionado no shell que o programa finalizado pelo signal de
        # código N deve retornar o código N + 128.
        sys.exit(130)
    except Exception as exc:
        LOGGER.exception(exc)
        sys.exit("An unexpected error has occurred: %s" % exc)


if __name__ == "__main__":
    main()
