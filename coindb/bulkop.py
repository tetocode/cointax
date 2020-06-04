import logging
from typing import Union, Sequence

from pymongo.collection import Collection
from pymongo.errors import BulkWriteError


class BulkOp:
    LIMIT = 1000

    def __init__(self, collection: Collection, logger: logging.Logger = None):
        self.collection = collection
        self.documents = []
        self.total_n = 0
        self.logger = logger or logging.Logger(self.__class__.__name__)

    def insert(self, doc_or_docs: Union[dict, Sequence[dict]]):
        if isinstance(doc_or_docs, Sequence):
            docs = doc_or_docs
        else:
            docs = [doc_or_docs]
        self.documents.extend(docs)
        if len(self.documents) >= self.LIMIT:
            self.execute()

    def execute(self):
        if len(self.documents):
            try:
                self.collection.insert_many(self.documents, ordered=False)
            except BulkWriteError as e:
                for i, error in enumerate(e.details['writeErrors']):
                    if error['code'] != 11000:
                        raise
                self.logger.info('duplicate entry found')
            finally:
                n = len(self.documents)
                self.total_n += n
                self.documents = []
                self.logger.info('bulk executed n={} total_n={}'.format(n, self.total_n))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self.execute()
