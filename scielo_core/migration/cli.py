import argparse
import json
import logging
import sys

from scielo_core import config
from scielo_core.migration import tasks, controller


LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(
    filename=config.SCIELO_CORE_MIGRATION_LOGFILE, level=logging.DEBUG)


def get_xml(v2):
    print(controller.get_xml(v2))


def register_migration(docs_jsonl_file_path, issns_file_path, skip_update=False):
    with open(docs_jsonl_file_path, mode="r", encoding="iso-8859-1") as fp:
        issns = set()
        for row in fp.readlines():
            try:
                LOGGER.info(row)
                data = json.loads(row.strip())
                issns.add(data["issn"])
                resp = tasks.register_migration(data, skip_update, True)
                LOGGER.info(resp)
            except (json.decoder.JSONDecodeError, UnicodeDecodeError) as e:
                LOGGER.exception(f"{row} {e}")
                with open("exceptions.log", "a") as fpexc:
                    fpexc.write(f"{row}\n")

    with open(issns_file_path, "w") as fp:
        fp.write("\n".join(issns))


def migrate(issns_file_path, xml_folder_path, collection):
    with open(issns_file_path) as fp:
        for issn in fp.readlines():
            issn = issn.strip()
            tasks.pull_data_and_request_id(issn, xml_folder_path, collection)


def request_id(issns_file_path):
    with open(issns_file_path) as fp:
        for issn in fp.readlines():
            issn = issn.strip()
            tasks.request_id_for_journal_documents(issn)


def undo_id_request(issns_file_path):
    with open(issns_file_path) as fp:
        for issn in fp.readlines():
            issn = issn.strip()
            tasks.undo_id_request_for_journal_documents(issn)


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
        "--skip_update",
        action='store_true',
        help="Skip update",
        default=False,
    )

    parser_register_migration.add_argument(
        "docs_jsonl_file_path",
        help="jsonl file path which contains some metadata from artigo.mst",
    )
    parser_register_migration.add_argument(
        "issns_file_path",
        help="file path to save an ISSN list",
    )

    parser_migrate = subparsers.add_parser(
        "migrate",
        help="Pull data",
        description="Pull data",
    )
    parser_migrate.add_argument(
        "issns_file_path",
        help="file path of ISSN list to pull data",
    )
    parser_migrate.add_argument(
        "--xml_folder_path",
        default=None,
        help="XML folder path",
    )
    parser_migrate.add_argument(
        "--collection",
        default=None,
        help="collection acronym",
    )

    parser_request_id = subparsers.add_parser(
        "request_id",
        help="Request ID",
        description="Request ID",
    )
    parser_request_id.add_argument(
        "issns_file_path",
        help="file path to save an ISSN list",
    )

    parser_undo_id_request = subparsers.add_parser(
        "undo_id_request",
        help="Undo ID request",
        description="Undo ID request",
    )
    parser_undo_id_request.add_argument(
        "issns_file_path",
        help="file path to save an ISSN list",
    )

    parser_get_xml = subparsers.add_parser(
        "get_xml",
        help="Get XML",
        description="Get XML",
    )
    parser_get_xml.add_argument(
        "v2",
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
    elif args.command == "migrate":
        migrate(
            args.issns_file_path, args.xml_folder_path, args.collection)
    elif args.command == "request_id":
        request_id(args.issns_file_path)
    elif args.command == "undo_id_request":
        undo_id_request(args.issns_file_path)
    elif args.command == "get_xml":
        get_xml(args.v2)
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
