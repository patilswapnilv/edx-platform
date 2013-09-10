"""
Exceptions thrown by KeyStore objects
"""
from xmodule.modulestore.django import modulestore


class ItemNotFoundError(Exception):
    pass


class ItemWriteConflictError(Exception):
    pass


class InsufficientSpecificationError(Exception):
    pass


class OverSpecificationError(Exception):
    pass


class InvalidLocationError(Exception):
    pass


class NoPathToItem(Exception):
    pass


class DuplicateItemError(Exception):
    """
    Attempted to create an item which already exists.
    """
    def __init__(self, element_id, store=modulestore(), collection=None):
        super(DuplicateItemError, self).__init__()
        self.element_id = element_id
        self.store = store
        self.collection = collection

    def __unicode__(self):
        try:
            return u"""
            Attempted to create element {self.element_id} in
            db {self.store.db} collection {self.collection}
            """.format(self)
        # pylint: disable=W0702
        except:
            return super(DuplicateItemError, self).__unicode__()

class VersionConflictError(Exception):
    """
    The caller asked for either draft or published head and gave a version which conflicted with it.
    """
    def __init__(self, requestedLocation, currentHead):
        super(VersionConflictError, self).__init__()
        self.requestedLocation = requestedLocation
        self.currentHead = currentHead
