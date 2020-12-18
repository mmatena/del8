"""Messages for communication between supervisor and worker."""
from del8.core import data_class


class MessageType(object):
    PROCESS_ITEM = "PROCESS_ITEM"
    ITEM_PROCESSED = "ITEM_PROCESSED"
    KILL = "KILL"


class ResponseStatus(object):
    SUCCESS = "SUCCESS"


@data_class.data_class()
class Message(object):
    def __init__(self, type, content=None):
        pass


###############################################################################


@data_class.data_class()
class ProcessItem(object):
    def __init__(self, execution_item):
        pass


@data_class.data_class()
class ItemProcessed(object):
    def __init__(self, status):
        pass
