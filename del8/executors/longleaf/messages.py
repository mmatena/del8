"""Messages for communication between supervisor and worker."""
from del8.core import data_class


class MessageType(object):
    PROCESS_ITEM = "PROCESS_ITEM"
    ITEM_PROCESSED = "ITEM_PROCESSED"


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

    @classmethod
    def from_execution_item(cls, execution_item):
        # NOTE: Returns an instance of Message, NOT an instance of this class.
        return Message(
            type=MessageType.PROCESS_ITEM,
            content=cls(execution_item=execution_item),
        )


@data_class.data_class()
class ItemProcessed(object):
    def __init__(self, status):
        pass
