import copy
import datetime
import io
import itertools
import sys
from collections.abc import Generator, Iterable, Iterator
from pathlib import Path
from typing import BinaryIO, Literal, Union
from uuid import UUID, uuid4

import pytest
import validators
from minio import Minio, S3Error
from minio.commonconfig import ENABLED, ComposeSource, CopySource, Tags
from minio.datatypes import Object
from minio.deleteobjects import (
    DeletedObject,
    DeleteError,
    DeleteObject,
    DeleteResult,
)
from minio.helpers import ObjectWriteResult, ProgressType
from minio.retention import Retention
from minio.sse import Sse, SseCustomerKey
from minio.versioningconfig import OFF, SUSPENDED, VersioningConfig
from urllib3._collections import HTTPHeaderDict
from urllib3.connection import HTTPConnection
from urllib3.response import HTTPResponse

from .exceptions import (
    invalid_version,
    method_not_allowed,
    no_such_bucket,
    no_such_key,
    no_such_version,
)
from .utils import _list_objects_checks


class MockMinioObjectVersion:
    def __init__(
        self,
        data: Union[BinaryIO, bytes],
        version_id: Union[UUID, Literal["null"]],
        is_delete_marker: bool,
        metadata: Union[dict, None] = None,
    ):
        if isinstance(data, bytes):
            self._size = len(data)
            data = io.BytesIO(data)
        else:
            self._size = len(data.read())
            data.seek(0)
        self._data = data
        self._version_id = version_id
        self._is_delete_marker = is_delete_marker
        self._metadata = metadata if metadata is not None else {}
        self._last_modified = datetime.datetime.now()

    @property
    def data(self) -> BinaryIO:
        return self._data

    @property
    def metadata(self) -> dict:
        return self._metadata

    @property
    def size(self) -> int:
        return self._size

    @property
    def version_id(self) -> Union[str, None]:
        return str(self._version_id) if self._version_id != "null" else None

    @property
    def last_modified(self) -> datetime.datetime:
        return self._last_modified

    @property
    def is_delete_marker(self) -> bool:
        return self._is_delete_marker

    @is_delete_marker.setter
    def is_delete_marker(self, value: bool):
        self._is_delete_marker = value


class MockMinioObject:
    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        data: Union[BinaryIO, bytes],
        length: int,
        content_type: str,
        metadata: Union[dict, None],
        sse: Union[Sse, None],
        progress: Union[ProgressType, None],
        part_size: int,
        num_parallel_uploads: int,
        tags: Union[Tags, None],
        retention: Union[Retention, None],
        legal_hold: bool,
        versioning: VersioningConfig,
    ):
        self._bucket_name = bucket_name
        self._object_name = object_name
        self._versions = {}
        self.latest_version_id = "null"
        self.put_object(
            data=data,
            length=length,
            content_type=content_type,
            metadata=metadata,
            sse=sse,
            progress=progress,
            part_size=part_size,
            num_parallel_uploads=num_parallel_uploads,
            tags=tags,
            retention=retention,
            legal_hold=legal_hold,
            versioning=versioning,
        )

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    @property
    def object_name(self) -> str:
        return self._object_name

    @property
    def versions(
        self,
    ) -> dict[Union[UUID, Literal["null"]], MockMinioObjectVersion]:
        if not self._versions:
            raise RuntimeError("Implementation error")
        return self._versions

    def get_latest(self) -> MockMinioObjectVersion:
        return self._versions[self.latest_version_id]

    def put_object_version(
        self,
        data: Union[BinaryIO, bytes] = io.BytesIO(b""),
        version_id: Union[UUID, Literal["null"]] = "null",
        is_delete_marker=False,
        metadata: Union[dict, None] = None,
    ):
        self.latest_version_id = version_id
        self._versions[self.latest_version_id] = MockMinioObjectVersion(
            data=data,
            version_id=version_id,
            is_delete_marker=is_delete_marker,
            metadata=metadata,
        )
        return self._versions[self.latest_version_id]

    def put_object(
        self,
        data: Union[BinaryIO, bytes],
        length: int,
        content_type: str,
        metadata: dict,
        sse: Sse,
        progress: ProgressType,
        part_size: int,
        num_parallel_uploads: int,
        tags: Tags,
        retention: Retention,
        legal_hold: bool,
        versioning: VersioningConfig = VersioningConfig(),
    ) -> ObjectWriteResult:
        # If versioning is OFF, there can only be one version of an object
        # (store a read version_id non-the-less, but the version_id is 'null')
        if versioning.status == OFF:
            self._versions = {}

        # According to
        # https://min.io/docs/minio/linux/administration/object-management/object-versioning.html#suspend-bucket-versioning
        # objects created when versioning is suspended have a 'null' version ID
        obj_version = self.put_object_version(
            data=data,
            version_id="null" if versioning.status != ENABLED else uuid4(),
            metadata=metadata,
        )
        return ObjectWriteResult(
            self.bucket_name,
            self.object_name,
            obj_version.version_id,
            None,
            HTTPHeaderDict(),
            obj_version.last_modified,
            None,
        )

    def get_object(
        self, version_id: Union[str, None], versioning: VersioningConfig
    ) -> MockMinioObjectVersion:
        if versioning.status == OFF:
            # Versioning is OFF if and only if the bucket has never been
            # versioned so only the 'null' version matters
            version_id = None

        version_id = self._check_version_id(version_id)
        if not version_id:
            if versioning.status == OFF:
                the_object = self._check_object_version("null")
            else:
                the_object = self.get_latest()
        else:
            the_object = self._check_object_version(version_id)

        # if the delete_marker is set raise an error
        if the_object.is_delete_marker:
            if version_id:
                raise method_not_allowed(self.bucket_name, self.object_name)
            raise no_such_key(self.bucket_name, self.object_name, True)
        return the_object

    def list_versions(
        self,
    ) -> list[tuple[Union[UUID, Literal["null"]], MockMinioObjectVersion]]:
        return sorted(
            self.versions.items(),
            key=lambda i: (
                i[1].is_delete_marker,
                -i[1].last_modified.timestamp(),
            ),
        )

    def remove_object(
        self, version_id: Union[str, None], versioning: VersioningConfig
    ):
        def _delete_version(v_):
            if version_id not in self.versions:
                # version_id does not exist, nothing to do
                return
            del self.versions[v_]
            if self._versions and v_ == self.latest_version_id:
                self.latest_version_id = list(self.versions.values())[  # noqa: SLF001
                    -1
                ]._version_id

        version_id = self._check_version_id(version_id)
        if versioning.status == ENABLED:
            if version_id:
                _delete_version(version_id)
                return
            # version_id is not specified, remove latest
            if self.get_latest().is_delete_marker:
                # nothing to do
                return
            version_id = uuid4()

            self.put_object_version(
                version_id=version_id, is_delete_marker=True
            )
            return

        if versioning.status == SUSPENDED:
            if version_id:
                _delete_version(version_id)
                return
            self.get_latest().is_delete_marker = True

    def stat_object(
        self,
        version_id: Union[str, None],
        versioning: VersioningConfig,
    ) -> Object:
        obj = self.get_object(version_id, versioning=versioning)
        return Object(
            self.bucket_name,
            self.object_name,
            last_modified=obj.last_modified,
            version_id=obj.version_id,
            size=obj.size,
            metadata=obj.metadata,
        )

    def _check_version_id(
        self, version_id: Union[str, None] = None
    ) -> Union[UUID, Literal["null"], None]:
        if not version_id:
            return None
        if version_id == "null":
            return "null"
        try:
            return UUID(version_id)
        except ValueError as error:
            raise invalid_version(self.bucket_name, self.object_name) from error

    def _check_object_version(
        self, version_id: Union[UUID, Literal["null"]]
    ) -> MockMinioObjectVersion:
        try:
            return self.versions[version_id]
        except KeyError as error:
            raise no_such_version(self.bucket_name, self.object_name) from error


class MockMinioBucket:
    def __init__(
        self,
        bucket_name: str,
        versioning: VersioningConfig,
        location: Union[str, None] = None,
        object_lock: bool = False,
    ):
        self._bucket_name = bucket_name
        self._versioning = versioning
        self._objects = {}
        self._location = location
        self._object_lock = object_lock

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    @property
    def objects(self) -> dict[str, MockMinioObject]:
        return self._objects

    @property
    def versioning(self) -> VersioningConfig:
        return self._versioning

    @versioning.setter
    def versioning(self, versioning: VersioningConfig):
        self._versioning = versioning

    def put_object(
        self,
        object_name: str,
        data: Union[BinaryIO, bytes],
        length: int,
        content_type: str,
        metadata: dict,
        sse: Sse,
        progress: ProgressType,
        part_size: int,
        num_parallel_uploads: int,
        tags: Tags,
        retention: Retention,
        legal_hold: bool,
    ) -> ObjectWriteResult:
        if object_name not in self.objects:
            self.objects[object_name] = MockMinioObject(
                self.bucket_name,
                object_name,
                data=data,
                length=length,
                content_type=content_type,
                metadata=metadata,
                sse=sse,
                progress=progress,
                part_size=part_size,
                num_parallel_uploads=num_parallel_uploads,
                tags=tags,
                retention=retention,
                legal_hold=legal_hold,
                versioning=self.versioning,
            )
            obj_version = self.objects[object_name].get_latest()
            return ObjectWriteResult(
                self.bucket_name,
                object_name,
                obj_version.version_id,
                None,
                HTTPHeaderDict(),
                obj_version.last_modified,
                None,
            )
        return self.objects[object_name].put_object(
            data=data,
            length=length,
            content_type=content_type,
            metadata=metadata,
            sse=sse,
            progress=progress,
            part_size=part_size,
            num_parallel_uploads=num_parallel_uploads,
            tags=tags,
            retention=retention,
            legal_hold=legal_hold,
            versioning=self.versioning,
        )

    def remove_object(
        self, object_name: str, version_id: Union[str, None] = None
    ):
        if object_name not in self.objects:
            # object does not exist, so nothing to do
            return
        if self.versioning.status == OFF:
            # Versioning if off if and only if it has never been enabled, so
            # the object is deleted completely
            del self.objects[object_name]
            return
        self.objects[object_name].remove_object(version_id, self.versioning)
        if not self.objects[object_name]._versions:  # noqa: SLF001
            # If the last version was deleted, remove the object from the
            # bucket entierly
            del self.objects[object_name]

    def get_object(
        self, object_name: str, version_id: Union[str, None] = None
    ) -> MockMinioObjectVersion:
        return self._check_object(object_name).get_object(
            version_id, self.versioning
        )

    def list_objects(
        self,
        continuation_token: Union[str, None] = None,
        delimiter: Union[str, None] = None,
        encoding_type: Union[str, None] = None,
        fetch_owner: Union[bool, None] = None,
        include_user_meta: bool = False,
        max_keys: Union[int, None] = None,
        prefix: Union[str, None] = None,
        start_after: Union[str, None] = None,
        version_id_marker: Union[str, None] = None,
        use_api_v1: bool = False,
        include_version: bool = False,
    ) -> Generator[Object, None, None]:
        start_after, recursive = _list_objects_checks(
            use_api_v1, start_after, delimiter
        )
        seen_prefixes = set()

        for object_name, obj in self.objects.items():
            if object_name.startswith(prefix) and (
                start_after == "" or object_name > start_after
            ):
                # Handle non-recursive listing by identifying and adding unique
                # directory names
                if not recursive:
                    sub_path = object_name[len(prefix) :].strip("/")
                    dir_end_idx = sub_path.find("/")
                    if dir_end_idx != -1:
                        dir_name = prefix + sub_path[: dir_end_idx + 1]
                        if dir_name not in seen_prefixes:
                            seen_prefixes.add(dir_name)
                            yield Object(
                                bucket_name=self.bucket_name,
                                object_name=dir_name,
                            )
                        # Skip further processing to prevent
                        # adding the full object path
                        continue
                # Directly add the object for recursive listing
                # or if it's a file in the current directory
                if include_version:
                    # Minio API always sort versions by time,
                    # it also includes delete markers at the end newest first
                    for version, obj_version in obj.list_versions():
                        yield Object(
                            bucket_name=self.bucket_name,
                            object_name=object_name,
                            last_modified=obj_version.last_modified,
                            version_id=obj_version.version_id,
                            is_latest=str(
                                version == obj.latest_version_id
                            ).lower(),
                            is_delete_marker=obj_version.is_delete_marker,
                            metadata=obj_version.metadata,
                        )
                elif not (obj_version := obj.get_latest()).is_delete_marker:
                    yield Object(
                        bucket_name=self.bucket_name,
                        object_name=object_name,
                        last_modified=obj_version.last_modified,
                        version_id=obj_version.version_id,
                        is_latest="true",
                        is_delete_marker=obj_version.is_delete_marker,
                        metadata=obj_version.metadata,
                    )

    def _check_object(self, object_name) -> MockMinioObject:
        try:
            return self.objects[object_name]
        except KeyError as error:
            raise no_such_key(self.bucket_name, object_name) from error

    def stat_object(
        self,
        object_name: str,
        version_id: Union[str, None] = None,
    ) -> Object:
        return self._check_object(object_name).stat_object(
            version_id, self.versioning
        )


class MockMinioServer:
    def __init__(self, endpoint: str):
        self._base_url = endpoint
        self._buckets = {}

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def buckets(self) -> dict[str, MockMinioBucket]:
        return self._buckets


class MockMinioServers:
    def __init__(self):
        self.servers: dict[str, MockMinioServer] = {}

    def connect(self, endpoint: str) -> MockMinioServer:
        if endpoint not in self.servers:
            self.servers[endpoint] = MockMinioServer(endpoint)
        return self.servers[endpoint]

    def reset(self):
        self.servers = {}


class MockMinioClient:
    def __init__(
        self,
        endpoint,
        access_key: Union[str, None] = None,
        secret_key: Union[str, None] = None,
        session_token: Union[str, None] = None,
        secure: bool = True,
        region: Union[str, None] = None,
        http_client=None,
        credentials=None,
    ):
        if not endpoint:
            raise ValueError("base_url is empty")
        if not validators.hostname(endpoint) and not validators.url(endpoint):
            raise ValueError(f"base_url {endpoint} is not valid")
        self._base_url = endpoint
        self._access_key = access_key
        self._secret_key = secret_key
        self._session_token = session_token
        self._secure = secure
        self._region = region
        self._http_client = http_client
        self._credentials = credentials
        self.buckets: dict[str, MockMinioBucket] = {}

    def connect(self, servers: MockMinioServers):
        self.buckets = servers.connect(self._base_url).buckets

    def fget_object(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        request_headers: Union[dict, None] = None,
        sse: Union[Sse, None] = None,
        version_id: Union[str, None] = None,
        extra_query_params: Union[dict, None] = None,
    ):
        the_object = self.get_object(
            bucket_name,
            object_name,
            version_id=version_id,
            request_headers=request_headers,
            sse=sse,
            extra_query_params=extra_query_params,
        )
        with Path(file_path).open("wb") as f:
            f.write(the_object.data)

    def get_object(
        self,
        bucket_name: str,
        object_name: str,
        offset: int = 0,
        length: int = 0,
        request_headers: Union[dict, None] = None,
        sse: Union[Sse, None] = None,
        version_id: Union[str, None] = None,
        extra_query_params: Union[dict, None] = None,
    ):
        data = (
            self.__check_bucket(bucket_name)
            .get_object(object_name, version_id)
            .data
        )

        # Create a buffer containing the data
        if isinstance(data, io.BytesIO):
            body = copy.deepcopy(data)
        elif isinstance(data, bytes):
            body = data
        elif isinstance(data, str):
            body = io.BytesIO(data.encode("utf-8"))
        else:
            body = data

        conn = HTTPConnection("localhost")
        return HTTPResponse(body=body, preload_content=False, connection=conn)

    def fput_object(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        content_type: str = "application/octet-stream",
        metadata: Union[dict, None] = None,
        sse: Union[Sse, None] = None,
        progress: Union[ProgressType, None] = None,
        part_size: int = 0,
        num_parallel_uploads: int = 3,
        tags: Union[Tags, None] = None,
        retention: Union[Retention, None] = None,
        legal_hold: bool = False,
    ) -> ObjectWriteResult:
        with Path(file_path).open("rb") as file_data:
            data = file_data.read()
        return self.put_object(
            bucket_name,
            object_name,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
            metadata=metadata,
            sse=sse,
            progress=progress,
            part_size=part_size,
            num_parallel_uploads=num_parallel_uploads,
            tags=tags,
            retention=retention,
            legal_hold=legal_hold,
        )

    def put_object(
        self,
        bucket_name: str,
        object_name: str,
        data: Union[BinaryIO, bytes],
        length: int,
        content_type: str = "application/octet-stream",
        metadata: Union[dict, None] = None,
        sse: Union[Sse, None] = None,
        progress: Union[ProgressType, None] = None,
        part_size: int = 0,
        num_parallel_uploads: int = 3,
        tags: Union[Tags, None] = None,
        retention: Union[Retention, None] = None,
        legal_hold: bool = False,
    ) -> ObjectWriteResult:
        return self.__check_bucket(bucket_name).put_object(
            object_name=object_name,
            data=data,
            length=length,
            content_type=content_type,
            metadata=metadata,
            sse=sse,
            progress=progress,
            part_size=part_size,
            num_parallel_uploads=num_parallel_uploads,
            tags=tags,
            retention=retention,
            legal_hold=legal_hold,
        )

    def get_presigned_url(
        self,
        method: Literal[
            "GET",
            "POST",
            "PUT",
            "HEAD",
            "DELETE",
            "PATCH",
            "OPTIONS",
            "CONNECT",
            "TRACE",
        ],
        bucket_name: str,
        object_name: str,
        expires: datetime.timedelta = datetime.timedelta(days=7),
        response_headers: Union[dict, None] = None,
        request_date: Union[datetime.datetime, None] = None,
        version_id: Union[str, None] = None,
        extra_query_params: Union[dict, None] = None,
    ) -> str:
        return (
            f"{self._base_url}/{bucket_name}/{object_name}"
            if not version_id
            else f"{self._base_url}/{bucket_name}/{object_name}?"
            f"versionId={version_id}"
        )

    def presigned_put_object(
        self,
        bucket_name: str,
        object_name: str,
        expires=datetime.timedelta(days=7),
    ) -> str:
        return self.get_presigned_url("PUT", bucket_name, object_name, expires)

    def presigned_get_object(
        self,
        bucket_name: str,
        object_name: str,
        expires=datetime.timedelta(days=7),
        response_headers: Union[dict, None] = None,
        request_date: Union[datetime.datetime, None] = None,
        version_id: Union[str, None] = None,
        extra_query_params: Union[dict, None] = None,
    ) -> str:
        return self.get_presigned_url(
            "GET",
            bucket_name,
            object_name,
            expires,
            response_headers=response_headers,
            request_date=request_date,
            version_id=version_id,
            extra_query_params=extra_query_params,
        )

    def list_buckets(self) -> list[str]:
        return list(self.buckets)

    def bucket_exists(self, bucket_name: str) -> bool:
        try:
            self.buckets[bucket_name]
        except KeyError:
            return False
        return True

    def make_bucket(
        self,
        bucket_name: str,
        location: Union[str, None] = None,
        object_lock: bool = False,
    ):
        self.buckets[bucket_name] = MockMinioBucket(
            bucket_name=bucket_name,
            versioning=VersioningConfig(),
            location=location,
            object_lock=object_lock,
        )
        return True

    def set_bucket_versioning(self, bucket_name: str, config: VersioningConfig):
        if not isinstance(config, VersioningConfig):
            raise TypeError("config must be VersioningConfig type")
        self.__check_bucket(bucket_name).versioning = config

    def get_bucket_versioning(self, bucket_name: str) -> VersioningConfig:
        return self.__check_bucket(bucket_name).versioning

    def _list_objects(
        self,
        bucket_name: str,
        continuation_token: Union[str, None] = None,
        delimiter: Union[str, None] = None,
        encoding_type: Union[str, None] = None,
        fetch_owner: Union[bool, None] = None,
        include_user_meta: bool = False,
        max_keys: Union[int, None] = None,
        prefix: Union[str, None] = None,
        start_after: Union[str, None] = None,
        version_id_marker: Union[str, None] = None,
        use_api_v1: bool = False,
        include_version: bool = False,
    ):
        return self.__check_bucket(bucket_name).list_objects(
            continuation_token=continuation_token,
            delimiter=delimiter,
            encoding_type=encoding_type,
            fetch_owner=fetch_owner,
            include_user_meta=include_user_meta,
            max_keys=max_keys,
            prefix=prefix,
            start_after=start_after,
            version_id_marker=version_id_marker,
            use_api_v1=use_api_v1,
            include_version=include_version,
        )

    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
        recursive: bool = False,
        start_after: str = "",
        include_user_meta: bool = False,
        include_version: bool = False,
        use_api_v1: bool = False,
        use_url_encoding_type: bool = True,
        fetch_owner: bool = False,
    ) -> Generator[Object, None, None]:
        return self._list_objects(
            bucket_name,
            delimiter=None if recursive else "/",
            include_user_meta=include_user_meta,
            prefix=prefix,
            start_after=start_after,
            use_api_v1=use_api_v1,
            include_version=include_version,
            encoding_type="url" if use_url_encoding_type else None,
            fetch_owner=fetch_owner,
        )

    def stat_object(
        self,
        bucket_name: str,
        object_name: str,
        ssec: Union[SseCustomerKey, None] = None,
        version_id: Union[str, None] = None,
        extra_headers: Union[dict, None] = None,
        extra_query_params: Union[dict, None] = None,
    ) -> Object:
        return self.__check_bucket(bucket_name).stat_object(
            object_name,
            version_id,
        )

    def compose_object(
        self,
        bucket_name: str,
        object_name: str,
        sources: list[ComposeSource],
        sse: Union[Sse, None] = None,
        metadata: Union[dict, None] = None,
        tags: Union[Tags, None] = None,
        retention: Union[Retention, None] = None,
        legal_hold: bool = False,
    ) -> ObjectWriteResult:
        if not isinstance(sources, (list, tuple)) or not sources:
            raise ValueError("sources must be non-empty list or tuple type")
        data = b""
        if metadata is None:
            metadata = {}
        metadata_ = {}
        for source in sources:
            data += self.get_object(
                source.bucket_name,
                source.object_name,
                version_id=source.version_id,
            ).data
            metadata_.update(
                self.buckets[source.bucket_name]
                .objects[source.object_name]
                .get_object(
                    source.version_id,
                    self.get_bucket_versioning(source.bucket_name),
                )
                .metadata
                | {}
            )
        metadata_.update(metadata)
        return self.put_object(
            bucket_name, object_name, data, length=len(data), metadata=metadata_
        )

    def copy_object(
        self,
        bucket_name: str,
        object_name: str,
        source: CopySource,
        sse: Union[Sse, None] = None,
        metadata: Union[dict, None] = None,
        tags: Union[Tags, None] = None,
        retention: Union[Retention, None] = None,
        legal_hold: bool = False,
        metadata_directive: Union[str, None] = None,
        tagging_directive: Union[str, None] = None,
    ) -> ObjectWriteResult:
        if metadata is None:
            metadata = {}
        data = self.get_object(
            source.bucket_name, source.object_name, version_id=source.version_id
        ).data
        metadata_ = (
            self.buckets[source.bucket_name]
            .objects[source.object_name]
            .get_object(
                source.version_id,
                self.get_bucket_versioning(source.bucket_name),
            )
            .metadata
        )
        if metadata_ is not None:
            metadata_.update(metadata)
        else:
            metadata_ = metadata
        return self.put_object(
            bucket_name, object_name, data, len(data), metadata=metadata_
        )

    def remove_object(
        self,
        bucket_name: str,
        object_name: str,
        version_id: Union[str, None] = None,
    ):
        return self.__check_bucket(bucket_name).remove_object(
            object_name, version_id=version_id
        )

    def remove_objects(
        self,
        bucket_name: str,
        delete_object_list: Iterable[DeleteObject],
        bypass_governance_mode: bool = False,
    ) -> Iterator[DeleteError]:
        self.__check_bucket(bucket_name)
        delete_object_list = itertools.chain(delete_object_list)
        kwargs = {"strict": False} if sys.version_info.minor != 9 else {}
        while True:
            # get 1000 entries or whatever available.
            objects = [
                delete_object
                for _, delete_object in zip(
                    range(1000), delete_object_list, **kwargs
                )
            ]

            if not objects:
                break

            result = self._delete_objects(
                bucket_name,
                objects,
                quiet=True,
                bypass_governance_mode=bypass_governance_mode,
            )

            for error in result.error_list:
                # AWS S3 returns "NoSuchVersion" error when
                # version doesn't exist ignore this error
                # yield all errors otherwise
                if error.code != "NoSuchVersion":
                    yield error

    def _delete_objects(
        self,
        bucket_name: str,
        delete_object_list: Iterable[DeleteObject],
        quiet: bool = False,
        bypass_governance_mode: bool = False,
    ) -> DeleteResult:
        bucket = self.__check_bucket(bucket_name)
        deleted = []
        errors = []
        for obj in delete_object_list:
            object_name = obj._name  # noqa: SLF001
            version_id = obj._version_id  # noqa: SLF001
            try:
                the_object = bucket._check_object(object_name=object_name)  # noqa: SLF001
                version_id_ = the_object._check_version_id(  # noqa: SLF001
                    version_id=version_id
                )
                bucket.remove_object(
                    object_name=object_name,
                    version_id=version_id,
                )
            except S3Error as error:
                errors.append(
                    DeleteError(
                        code=error.code,
                        message=error.message,
                        name=object_name,
                        version_id=version_id,
                    )
                )
                break
            except Exception as error:
                errors.append(
                    DeleteError(
                        code=error.__class__.__name__,
                        message=str(error),
                        name=object_name,
                        version_id=version_id,
                    )
                )
                break
            else:
                # See boto3's documentation to understand the weird meaning of
                # delete_marker
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_objects.html
                delete_marker = False
                delete_marker_version_id = None
                if version_id_ is None and (
                    delete_marker := the_object.get_latest().is_delete_marker
                ):
                    delete_marker_version_id = str(the_object.latest_version_id)
                    version_id = None
                deleted.append(
                    DeletedObject(
                        name=object_name,
                        version_id=version_id,
                        delete_marker=delete_marker,
                        delete_marker_version_id=delete_marker_version_id,
                    )
                )
        return DeleteResult(deleted, errors)

    def __check_bucket(self, bucket_name: str) -> MockMinioBucket:
        try:
            return self.buckets[bucket_name]
        except KeyError as error:
            raise no_such_bucket(bucket_name) from error


@pytest.fixture
def minio_mock_servers():
    return MockMinioServers()


@pytest.fixture
def minio_mock(mocker, minio_mock_servers):
    def minio_mock_init(
        cls,
        *args,
        **kwargs,
    ):
        client = MockMinioClient(*args, **kwargs)
        client.connect(minio_mock_servers)
        return client

    return mocker.patch.object(Minio, "__new__", new=minio_mock_init)
