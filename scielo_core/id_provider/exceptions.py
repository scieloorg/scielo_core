
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


class SavingError(Exception):
    ...


class RequestDocumentIdError(Exception):
    ...


class GetRegisteredDocumentError(Exception):
    ...


class DocumentDoesNotExistError(Exception):
    ...


class DocumentIsUpdatedError(Exception):
    ...


class NotFoundXMLError(Exception):
    ...


class UnableToCreateXMLError(Exception):
    ...
