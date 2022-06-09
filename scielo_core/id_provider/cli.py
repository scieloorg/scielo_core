import sys
import argparse
import logging

from scielo_core.id_provider import tasks, view
from scielo_core.config import run_concurrently


LOGGER = logging.getLogger(__name__)
LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def _request_id(pkg_file_path):
    if run_concurrently():
        return tasks.request_id(pkg_file_path, get_result=False)
    return view.request_document_id(pkg_file_path)


def _request_id_for_a_xml_list(source_file_path, output_file_path):
    with open(source_file_path) as fp:
        for row in fp.readlines():
            resp = _request_id(row.strip())
            with open(output_file_path, "a") as out:
                out.write(row)


def get_xml(v3):
    print(view.get_xml(v3))


def cli(argv=None):
    if argv is None:
        argv = sys.argv
    parser = argparse.ArgumentParser(
        description="SciELO ID Provider command line utility.",
    )
    # parser.add_argument("-v", "--version", action="version", version=VERSION)
    parser.add_argument("--loglevel", default="")

    subparsers = parser.add_subparsers(
        title="Commands", metavar="", dest="command",
    )

    parser_request_id = subparsers.add_parser(
        "request_id",
        help="Request ID for a bunch of XML files",
        description="Request ID for a bunch of XML files",
    )
    parser_request_id.add_argument(
        "source_file_path", help="file path which contains a list of XML files"
    )
    parser_request_id.add_argument(
        "output_file_path", help="result"
    )

    parser_get_xml = subparsers.add_parser(
        "get_xml",
        help="Request ID for a bunch of XML files",
        description="Request ID for a bunch of XML files",
    )
    parser_get_xml.add_argument(
        "v3", help="v3"
    )

    args = parser.parse_args()
    # todas as mensagens serão omitidas se level > 50
    logging.basicConfig(
        level=getattr(logging, args.loglevel.upper(), 999), format=LOGGER_FMT
    )
    if args.command == "request_id":
        _request_id_for_a_xml_list(args.source_file_path, args.output_file_path)
    elif args.command == "get_xml":
        get_xml(args.v3)
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
