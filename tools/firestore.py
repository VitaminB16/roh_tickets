from __future__ import annotations
import os
import json
import concurrent.futures
from google.cloud import firestore

from cloud.utils import log
from python_roh.src.utils import is_dataframe


class Firestore:
    """
    Class for operating Firestore
    """

    def __init__(self, path=None, project=None):
        """
        Args:
        - path (str): Path to the Firestore document or collection
        - project (str): Project ID

        Examples:
        - Firestore("bucket/path").read() -> Read from Firestore "bucket/path"
        - Firestore("gs://project/bucket/path").read() -> Read from Firestore "bucket/path"
        - Firestore("gs://project/bucket/path").write(data) -> Write to Firestore "bucket/path"
        - Firestore("gs://project/bucket/path").delete() -> Delete from Firestore "bucket/path"
        """
        self.project = project or os.getenv("PROJECT")
        self.client = firestore.Client(self.project)
        self.path = path

    def _parse_path(self, method="get"):
        """
        Parse the path into project, bucket, and path.
        This allows Firestore to be used in the same way as GCS.
        """
        if self.path is None:
            return None
        if "gs://" in self.path:
            # gs://project/bucket/path -> project, bucket, path
            # path -> collection/document/../collection/document
            self.path = self.path.replace("gs://", "")
            self.bucket, self.path = self.path.split("/", 1)
        path_elements = self.path.split("/")
        return path_elements

    def get_ref(self, method=None):
        path_elements = self._parse_path(method)
        if path_elements is None:
            return None
        doc_ref = self.client.collection(path_elements.pop(0))
        ref_type = "document"
        while len(path_elements) > 0:
            doc_ref = getattr(doc_ref, ref_type)(path_elements.pop(0))
            ref_type = "document" if ref_type == "collection" else "collection"
        return doc_ref

    def async_read(self, paths_list, allow_empty=False, apply_schema=False, schema={}):
        """
        Read from Firestore asynchronously
        Args:
        - paths_list (list): List of paths to read from Firestore
        - allow_empty (bool): If True, return an empty DataFrame if the document is empty
        - apply_schema (bool): If True, apply the schema from FIRESTORE_SCHEMAS. Also converts the output to a DataFrame.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [
                executor.submit(
                    Firestore(path).read,
                    allow_empty=allow_empty,
                    apply_schema=apply_schema,
                    schema=schema,
                )
                for path in paths_list
            ]
            concurrent.futures.wait(futures)
            output = {
                path.split("/")[-1]: future.result()
                for path, future in zip(paths_list, futures)
            }
        return output

    def read(self, allow_empty=False, apply_schema=False, schema={}):
        """
        Read from Firestore
        Args:
        - allow_empty (bool): If True, return an empty DataFrame if the document is empty
        - apply_schema (bool): If True, apply the schema from FIRESTORE_SCHEMAS. Also converts the output to a DataFrame.
        """
        log(f"Reading from Firestore: {self.path}")
        doc_ref = self.get_ref(method="get")
        if self._ref_type(doc_ref) == "collection":
            # If the reference is a collection, return a list of documents
            paths_list = [f"{self.path}/{doc.id}" for doc in doc_ref.stream()]
            return self.async_read(
                paths_list,
                allow_empty=allow_empty,
                apply_schema=apply_schema,
                schema=schema,
            )
        output = doc_ref.get().to_dict()
        metadata = {}
        object_type = None
        dtypes = {}
        if output is None and allow_empty:
            output = {}
        if set(output.keys()) in [{"data"}, {"data", "metadata"}]:
            metadata = output.get("metadata", {})
            object_type = metadata.get("object_type", None)
            dtypes = metadata.get("dtypes", {})
            output = output.get("data", output)
            if not dtypes:
                apply_schema = True
        if apply_schema:
            from python_roh.src.config import FIRESTORE_SCHEMAS

            schema = FIRESTORE_SCHEMAS.get(self.path, {})
            # print(object_type)
            # exit()
            if object_type == "<class 'pandas.core.frame.DataFrame'>":
                from pandas import DataFrame

                output = DataFrame(output)
                output = output.reset_index(drop=True)
            from python_roh.src.utils import enforce_schema

            output = enforce_schema(output, schema=schema, dtypes=dtypes)
        return output

    def write(self, data, columns=None):
        """
        Write to Firestore
        Args:
        - data (DataFrame or dict): Data to write to Firestore
        - columns (list): Columns to write from the DataFrame
        Returns:
        - True if successful
        """
        log(f"Writing to Firestore: {self.path}")
        doc_ref = self.get_ref(method="set")
        dtypes, object_type = None, None
        object_type = str(type(data))
        if is_dataframe(data):
            if columns is not None:
                data = data[columns]
            data.reset_index(drop=True, inplace=True)
            dtypes = data.dtypes.astype(str).to_dict()
            data = data.to_json()
        try:
            doc_ref.set(data)
        except ValueError as e:
            # This happens if the data is a dictionary with integer keys
            doc_ref.set(json.loads(json.dumps(data)))
        except AttributeError as e:
            # This happens if the data is a list of dictionaries
            if isinstance(data, str):
                # This happens if the data came from DataFrame.to_json()
                data = json.loads(data)
            output = {"data": data, "metadata": {}}
            if dtypes is not None:
                output["metadata"]["dtypes"] = dtypes
            if object_type is not None:
                output["metadata"]["object_type"] = object_type
            doc_ref.set(output)

        return True

    def delete(self):
        """
        Recursively deletes documents and collections from Firestore.
        """
        ref = self.get_ref()
        ref_type = self._ref_type(ref)
        if ref_type == "document":
            self._delete_document(ref)
        elif ref_type == "collection":
            self._delete_collection(ref)
        else:
            raise ValueError("Unsupported Firestore reference type.")
        print(f"Deleted {ref_type} from Firestore: {self.path}")
        return True

    def _delete_document(self, doc_ref):
        """
        Deletes a document and all of its collections.
        """
        collections = doc_ref.collections()
        for collection in collections:
            self._delete_collection(collection)
        doc_ref.delete()

    def _delete_collection(self, col_ref):
        """
        Deletes a collection and all of its documents.
        """
        docs = col_ref.stream()
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [
                executor.submit(self._delete_document, doc.reference) for doc in docs
            ]
            concurrent.futures.wait(futures)

    def _ref_type(self, doc_ref):
        is_doc_ref = isinstance(doc_ref, firestore.DocumentReference)
        is_coll_ref = isinstance(doc_ref, firestore.CollectionReference)
        return "document" if is_doc_ref else "collection" if is_coll_ref else None

    def ls(self, path=None) -> list[str]:
        """
        List all documents in a collection or all collections in a document.
        """
        if path is not None:
            self.path = self.path + "/" + path
        ref = self.get_ref()
        if ref is None:
            return [col.id for col in self.client.collections()]
        ref_type = self._ref_type(ref)
        if ref_type == "document":
            return [doc.id for doc in ref.collections()]
        elif ref_type == "collection":
            return [doc.id for doc in ref.stream()]
        else:
            raise ValueError("Unsupported Firestore reference type.")
