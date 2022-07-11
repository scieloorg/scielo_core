
class QueryingDocumentInIssueError(Exception):
    ...


class QueryingDocumentAsAOPError(Exception):
    ...


class FetchRecordsError(Exception):
    ...


class FetchMostRecentRecordError(Exception):
    ...


class TryToGetDocumentRecordError(Exception):
    ...


class NotEnoughParametersToGetDocumentRecordError(Exception):
    ...


class SavingError(Exception):
    ...


class RequestDocumentIdError(Exception):
    ...


class GetRegisteredDocumentError(Exception):
    ...


class DocumentDoesNotExistError(Exception):
    ...


class DocumentIsAlreadyUpdatedError(Exception):
    ...


class NotFoundXMLError(Exception):
    ...


class UnableToCreateXMLError(Exception):
    ...


class InputDataError(Exception):
    ...


class InvalidXMLError(Exception):
    ...

