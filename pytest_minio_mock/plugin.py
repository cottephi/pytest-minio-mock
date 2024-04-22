import copy
import datetime
import io
from collections.abc import Generator
from pathlib import Path
from typing import BinaryIO, Literal
from uuid import UUID, uuid4

import pytest
import validators
from minio import Minio
from minio.commonconfig import ENABLED, Tags
from minio.datatypes import Object
from minio.helpers import ProgressType
from minio.retention import Retention
from minio.sse import Sse, SseCustomerKey
from minio.versioningconfig import OFF, SUSPENDED, VersioningConfig
from urllib3.connection import HTTPConnection
from urllib3.response import HTTPResponse

from .exceptions import (
    invalid_version,
    method_not_allowed,
    no_such_bucket,
    no_such_key,
    no_such_version,
)


class MockMinioObjectVersion:
    def __init__(
        self,
        data: BinaryIO | bytes,
        version_id: UUID | Literal["null"],
        is_delete_marker: bool,
        metadata: dict | None = None,
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
    def version_id(self) -> str | None:
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
        data: BinaryIO | bytes,
        length: int,
        content_type: str,
        metadata: dict | None,
        sse: Sse | None,
        progress: ProgressType | None,
        part_size: int,
        num_parallel_uploads: int,
        tags: Tags | None,
        retention: Retention | None,
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
    def versions(self) -> dict[UUID | Literal["null"], MockMinioObjectVersion]:
        if not self._versions:
            raise RuntimeError("Implementation error")
        return self._versions

    def get_latest(self) -> MockMinioObjectVersion:
        return self._versions[self.latest_version_id]

    def put_object_version(
        self,
        data: BinaryIO | bytes = io.BytesIO(b""),
        version_id: UUID | Literal["null"] = "null",
        is_delete_marker=False,
        metadata: dict | None = None,
    ):
        self.latest_version_id = version_id
        self._versions[self.latest_version_id] = MockMinioObjectVersion(
            data=data,
            version_id=version_id,
            is_delete_marker=is_delete_marker,
            metadata=metadata,
        )

    def put_object(
        self,
        data: BinaryIO | bytes,
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
    ):
        # If versioning is OFF, there can only be one version of an object
        # (store a read version_id non-the-less, but the version_id is 'null')
        if versioning.status == OFF:
            self._versions = {}

        # According to
        # https://min.io/docs/minio/linux/administration/object-management/object-versioning.html#suspend-bucket-versioning
        # objects created when versioning is suspended have a 'null' version ID
        return self.put_object_version(
            data=data,
            version_id="null" if versioning.status != ENABLED else uuid4(),
            metadata=metadata,
        )

    def get_object(
        self, version_id: str | None, versioning: VersioningConfig
    ) -> MockMinioObjectVersion:
        if versioning.status == OFF:
            # Versioning is OFF if and only if the bucket has never been
            # versioned so only the 'null' version matters
            version_id = None

        version_id = self.__check_version_id(version_id)
        if not version_id:
            if versioning.status == OFF:
                the_object = self.__check_object_version("null")
            else:
                the_object = self.get_latest()
        else:
            the_object = self.__check_object_version(version_id)

        # if the delete_marker is set raise an error
        if the_object.is_delete_marker:
            raise method_not_allowed(self.bucket_name, self.object_name)
        return the_object

    def list_versions(
        self,
    ) -> list[tuple[UUID | Literal["null"], MockMinioObjectVersion]]:
        return sorted(
            self.versions.items(),
            key=lambda i: (
                i[1].is_delete_marker,
                -i[1].last_modified.timestamp(),
            ),
        )

    def remove_object(
        self, version_id: str | None, versioning: VersioningConfig
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

        version_id = self.__check_version_id(version_id)
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
        version_id: str | None = None,
    ) -> Object:
        version_id = self.__check_version_id(version_id, exclude_none=True)
        obj = self.__check_object_version(version_id)
        return Object(
            self.bucket_name,
            self.object_name,
            last_modified=obj.last_modified,
            version_id=obj.version_id,
            size=obj.size,
            metadata=obj.metadata,
        )

    def __check_version_id(
        self, version_id: str | None = None, exclude_none: bool = False
    ) -> UUID | Literal["null"] | None:
        if not version_id:
            return None if not exclude_none else "null"
        if version_id == "null":
            return "null"
        try:
            return UUID(version_id)
        except ValueError as error:
            raise invalid_version(self.bucket_name, self.object_name) from error

    def __check_object_version(
        self, version_id: UUID | Literal["null"]
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
        location: str | None = None,
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
        data: BinaryIO | bytes,
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
    ):
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
        else:
            self.objects[object_name].put_object(
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

    def remove_object(self, object_name: str, version_id: str | None = None):
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
        self, object_name: str, version_id: str | None = None
    ) -> MockMinioObjectVersion:
        return self.__check_object(object_name).get_object(
            version_id, self.versioning
        )

    def list_objects(
        self,
        prefix: str,
        recursive: bool,
        start_after: str,
        include_version: bool,
    ) -> Generator[Object, None, None]:
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

    def __check_object(self, object_name) -> MockMinioObject:
        try:
            return self.objects[object_name]
        except KeyError as error:
            raise no_such_key(self.bucket_name, object_name) from error

    def stat_object(
        self,
        object_name: str,
        version_id: str | None = None,
    ) -> Object:
        return self.__check_object(object_name).stat_object(version_id)


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
        access_key: str | None = None,
        secret_key: str | None = None,
        session_token: str | None = None,
        secure: bool = True,
        region: str | None = None,
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
        request_headers: dict | None = None,
        sse: Sse | None = None,
        version_id: str | None = None,
        extra_query_params: dict | None = None,
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
        request_headers: dict | None = None,
        sse: Sse | None = None,
        version_id: str | None = None,
        extra_query_params: dict | None = None,
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
        metadata: dict | None = None,
        sse: Sse | None = None,
        progress: ProgressType | None = None,
        part_size: int = 0,
        num_parallel_uploads: int = 3,
        tags: Tags | None = None,
        retention: Retention | None = None,
        legal_hold: bool = False,
    ):
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
        data: BinaryIO | bytes,
        length: int,
        content_type: str = "application/octet-stream",
        metadata: dict | None = None,
        sse: Sse | None = None,
        progress: ProgressType | None = None,
        part_size: int = 0,
        num_parallel_uploads: int = 3,
        tags: Tags | None = None,
        retention: Retention | None = None,
        legal_hold: bool = False,
    ):
        self.__check_bucket(bucket_name).put_object(
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

        return "Upload successful"

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
        response_headers: dict | None = None,
        request_date: datetime.datetime | None = None,
        version_id: str | None = None,
        extra_query_params: dict | None = None,
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
        response_headers: dict | None = None,
        request_date: datetime.datetime | None = None,
        version_id: str | None = None,
        extra_query_params: dict | None = None,
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
        location: str | None = None,
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

    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
        recursive: bool = False,
        start_after: str = "",
        include_version: bool = False,
    ) -> Generator[Object, None, None]:
        return self.__check_bucket(bucket_name).list_objects(
            prefix, recursive, start_after, include_version
        )

    def stat_object(
        self,
        bucket_name: str,
        object_name: str,
        ssec: SseCustomerKey | None = None,
        version_id: str | None = None,
        extra_headers: dict | None = None,
        extra_query_params: dict | None = None,
    ) -> Object:
        return self.__check_bucket(bucket_name).stat_object(
            object_name,
            version_id,
        )

    def remove_object(
        self, bucket_name: str, object_name: str, version_id: str | None = None
    ):
        return self.__check_bucket(bucket_name).remove_object(
            object_name, version_id=version_id
        )

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
