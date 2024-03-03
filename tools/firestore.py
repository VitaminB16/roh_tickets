import os
import json
import concurrent.futures
from pandas import DataFrame
from google.cloud import firestore

from cloud.utils import log


class Firestore:
    def __init__(self, path=None, project=None):
        self.project = project or os.getenv("PROJECT")
        self.client = firestore.Client(self.project)
        self.path = path

    def parse_path(self, method="get"):
        if "gs://" in self.path:
            # gs://project/bucket/path -> project, bucket, path
            # path -> collection/document/../collection/document
            self.path = self.path.replace("gs://", "")
            self.bucket, self.path = self.path.split("/", 1)
        path_elements = self.path.split("/")
        return path_elements

    def get_ref(self, method=None):
        path_elements = self.parse_path(method)
        doc_ref = self.client.collection(path_elements.pop(0))
        ref_type = "document"
        while len(path_elements) > 0:
            doc_ref = getattr(doc_ref, ref_type)(path_elements.pop(0))
            ref_type = "document" if ref_type == "collection" else "collection"
        print(f"Firestore {ref_type} reference: {self.path}")
        return doc_ref

    def read(self, allow_empty=False, apply_schema=False):
        from python_roh.src.config import FIRESTORE_SCHEMAS

        doc_ref = self.get_ref(method="get")
        output = doc_ref.get().to_dict()
        if output is None and allow_empty:
            output = {}
        if apply_schema:
            schema = FIRESTORE_SCHEMAS.get(self.path, None)
            df = DataFrame(output)
            from python_roh.src.utils import enforce_schema

            if schema is not None:
                output = enforce_schema(df, schema)
            else:
                output = df
        log(f"Read from Firestore: {self.path}")
        return output

    def write(self, data, columns=None):
        doc_ref = self.get_ref(method="set")
        if isinstance(data, DataFrame):
            if columns is not None:
                data = data[columns]
            data = json.loads(data.to_json())
        try:
            doc_ref.set(data)
        except ValueError as e:
            data = json.loads(json.dumps(data))
            doc_ref.set(data)
        log(f"Written to Firestore: {self.path}")
        return True

    def delete(self):
        """
        Recursively deletes documents and collections from Firestore.
        """
        ref = self.get_ref()
        ref_type = self.ref_type(ref)
        if ref_type == "document":
            self.delete_document(ref)
        elif ref_type == "collection":
            self.delete_collection(ref)
        else:
            raise ValueError("Unsupported Firestore reference type.")
        print(f"Deleted {ref_type} from Firestore: {self.path}")
        return True

    def delete_document(self, doc_ref):
        """
        Deletes a document and all of its collections.
        """
        collections = doc_ref.collections()
        for collection in collections:
            self.delete_collection(collection)
        doc_ref.delete()

    def delete_collection(self, col_ref):
        """
        Deletes a collection and all of its documents.
        """
        docs = col_ref.stream()
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [
                executor.submit(self.delete_document, doc.reference) for doc in docs
            ]
            concurrent.futures.wait(futures)

    def ref_type(self, doc_ref):
        is_doc_ref = isinstance(doc_ref, firestore.DocumentReference)
        is_coll_ref = isinstance(doc_ref, firestore.CollectionReference)
        return "document" if is_doc_ref else "collection" if is_coll_ref else None
