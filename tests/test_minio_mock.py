import sys

import pytest
import validators
from expects import (
    be_a,
    be_below,
    be_false,
    be_none,
    be_true,
    equal,
    expect,
    have_key,
    have_len,
)
from minio import Minio
from minio.commonconfig import ENABLED, ComposeSource, CopySource
from minio.deleteobjects import (
    DeletedObject,
    DeleteObject,
    DeleteResult,
)
from minio.error import S3Error
from minio.helpers import ObjectWriteResult
from minio.versioningconfig import OFF, SUSPENDED, VersioningConfig

from pytest_minio_mock.plugin import MockMinioBucket, MockMinioObject


class TestsMockMinioObject:
    def test_mock_minio_object_init(self):
        mock_minio_object = MockMinioObject(
            "test-bucket",
            "test-object",
            b"",
            0,
            "application/octet-stream",
            None,
            None,
            None,
            0,
            3,
            None,
            None,
            False,
            VersioningConfig(),
        )
        expect(mock_minio_object.versions).to(have_key("null"))


class TestsMockMinioBucket:
    def test_mock_minio_bucket_init(self):
        mock_minio_bucket = MockMinioBucket(
            bucket_name="test-bucket", versioning=VersioningConfig()
        )
        expect(mock_minio_bucket.bucket_name).to(equal("test-bucket"))
        expect(mock_minio_bucket.versioning.status).to(equal(OFF))
        expect(mock_minio_bucket.objects).to(equal({}))

        versioning_config = VersioningConfig(ENABLED)
        mock_minio_bucket = MockMinioBucket(
            bucket_name="test-bucket", versioning=versioning_config
        )
        expect(mock_minio_bucket._versioning).to(be_a(VersioningConfig))
        expect(mock_minio_bucket.versioning.status).to(equal(ENABLED))

    def test_versioning(self):
        mock_minio_bucket = MockMinioBucket(
            bucket_name="test-bucket", versioning=VersioningConfig()
        )
        versioning_config = mock_minio_bucket.versioning
        expect(versioning_config).to(be_a(VersioningConfig))
        expect(versioning_config.status).to(equal(OFF))
        versioning_config = VersioningConfig(status=ENABLED)
        mock_minio_bucket.versioning = versioning_config
        versioning_config = mock_minio_bucket.versioning
        expect(versioning_config).to(be_a(VersioningConfig))
        expect(versioning_config.status).to(equal(ENABLED))


def test_make_bucket(minio_mock):
    bucket_name = "test-bucket"
    client = Minio("http://local.host:9000")
    expect(client.bucket_exists(bucket_name)).to(be_false)
    client.make_bucket(bucket_name)
    expect(client.bucket_exists(bucket_name)).to(be_true)


@pytest.mark.FUNC()
def test_putting_and_removing_objects_no_versionning(minio_mock):
    # simple thing
    bucket_name = "test-bucket"
    object_name = "test-object"
    file_path = "tests/fixtures/maya.jpeg"

    client = Minio("http://local.host:9000")
    client.make_bucket(bucket_name)
    client.fput_object(bucket_name, object_name, file_path)
    client.fput_object(bucket_name, object_name, file_path)

    expect(client.buckets[bucket_name].objects).to(have_key(object_name))
    expect(client.list_objects(bucket_name)).to(have_len(1))
    client.remove_object(bucket_name, object_name)
    expect(client.buckets[bucket_name].objects).not_to(have_key(object_name))
    expect(client.list_objects(bucket_name)).to(have_len(0))

    # even if include version is True nothing should change because versioning
    # is OFF
    expect(client.list_objects(bucket_name, include_version=True)).to(
        have_len(0)
    )

    # test retrieving object after it has been removed
    with pytest.raises(S3Error, match="does not exist"):
        _ = client.get_object(bucket_name, object_name)

    client.fput_object(bucket_name, object_name, file_path)

    to_delete = [DeleteObject(object_name, None)]
    multiple_delete_result = client._delete_objects(bucket_name, to_delete)
    expect(multiple_delete_result).to(be_a(DeleteResult))
    expect(multiple_delete_result.error_list).to(have_len(0))
    expect(multiple_delete_result.object_list).to(have_len(1))

    expect(multiple_delete_result.object_list[0]).to(be_a(DeletedObject))
    expect(multiple_delete_result.object_list[0].name).to(equal(object_name))
    expect(multiple_delete_result.object_list[0].version_id).to(be_none)
    expect(multiple_delete_result.object_list[0].delete_marker).to(be_false)
    expect(multiple_delete_result.object_list[0].delete_marker_version_id).to(
        be_none
    )


@pytest.mark.FUNC()
def test_putting_objects_with_versionning_enabled(minio_mock):
    client = Minio("http://local.host:9000")
    bucket_name = "test-bucket"
    object_name = "test-object"
    file_path = "tests/fixtures/maya.jpeg"
    client.make_bucket(bucket_name)
    # Versioning Enabled
    client.set_bucket_versioning(bucket_name, VersioningConfig(ENABLED))
    # Add 3 objects
    client.fput_object(bucket_name, object_name, file_path)
    client.fput_object(bucket_name, object_name, file_path)
    client.fput_object(bucket_name, object_name, file_path)
    # there should be 3 versions of the same object
    expect(
        client.list_objects(bucket_name, object_name, include_version=False)
    ).to(have_len(1))
    # check that versions are stored correctly and retrieved correctly
    expect(
        client.list_objects(bucket_name, object_name, include_version=True)
    ).to(have_len(3))
    with pytest.raises(S3Error, match="Invalid version"):
        client.get_object(bucket_name, object_name, version_id="wrong")


@pytest.mark.FUNC()
def test_removing_object_version_with_versionning_enabled(minio_mock):
    client = Minio("http://local.host:9000")
    bucket_name = "test-bucket"
    object_name = "test-object"
    file_path = "tests/fixtures/maya.jpeg"
    client.make_bucket(bucket_name)

    # Versioning Enabled
    client.set_bucket_versioning(bucket_name, VersioningConfig(ENABLED))
    # Add 3 objects
    client.fput_object(bucket_name, object_name, file_path)
    client.fput_object(bucket_name, object_name, file_path)
    client.fput_object(bucket_name, object_name, file_path)

    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )
    newest_version = objects[0].version_id
    middle_version = objects[1].version_id
    oldest_version = objects[2].version_id
    expect(objects[1].last_modified).to(be_below(objects[0].last_modified))
    expect(objects[2].last_modified).to(be_below(objects[1].last_modified))
    expect(objects[0].is_latest).to(equal("true"))
    expect(objects[1].is_latest).to(equal("false"))
    expect(objects[2].is_latest).to(equal("false"))
    versions = list(
        client.buckets[bucket_name].objects[object_name].versions.values()
    )

    for i in range(len(objects)):
        expect(objects[i].version_id).to(equal(versions[2 - i].version_id))

    client.remove_object(bucket_name, object_name, version_id=oldest_version)
    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )
    expect(objects).to(have_len(2))
    expect(objects[0].version_id).to(equal(newest_version))
    expect(objects[1].version_id).to(equal(middle_version))
    expect(objects[0].is_latest).to(equal("true"))
    expect(objects[1].is_latest).to(equal("false"))
    versions = list(
        client.buckets[bucket_name].objects[object_name].versions.values()
    )

    for i in range(len(objects)):
        expect(objects[i].version_id).to(equal(versions[1 - i].version_id))

    client.remove_object(bucket_name, object_name, version_id=newest_version)
    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )
    expect(objects).to(have_len(1))
    expect(objects[0].version_id).to(equal(middle_version))
    expect(objects[0].is_latest).to(equal("true"))
    versions = list(
        client.buckets[bucket_name].objects[object_name].versions.values()
    )

    expect(objects[0].version_id).to(equal(versions[0].version_id))

    client.fput_object(bucket_name, object_name, file_path)
    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )
    expect(objects).to(have_len(2))
    expect(objects[0].version_id).not_to(equal(middle_version))
    expect(objects[1].version_id).to(equal(middle_version))
    versions = list(
        client.buckets[bucket_name].objects[object_name].versions.values()
    )

    for i in range(len(objects)):
        expect(objects[i].version_id).to(equal(versions[1 - i].version_id))


@pytest.mark.FUNC()
def test_putting_and_removing_and_listing_objects_with_versionning_enabled(  # noqa: PLR0915
    minio_mock,
):
    client = Minio("http://local.host:9000")
    bucket_name = "test-bucket"
    object_name = "test-object"
    file_path = "tests/fixtures/maya.jpeg"
    client.make_bucket(bucket_name)

    # Versioning Enabled
    client.set_bucket_versioning(bucket_name, VersioningConfig(ENABLED))
    # Add 3 objects
    client.fput_object(bucket_name, object_name, file_path)
    client.fput_object(bucket_name, object_name, file_path)
    client.fput_object(bucket_name, object_name, file_path)
    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )
    expect(objects).to(have_len(3))
    # removing the object with versioning enabled will add a delete marker
    client.remove_object(bucket_name, object_name)

    expect(
        list(
            client.list_objects(bucket_name, object_name, include_version=True)
        )
    ).to(have_len(4))

    expect(list(client.list_objects(bucket_name, object_name))).to(have_len(0))
    versions = list(
        client.buckets[bucket_name].objects[object_name].versions.values()
    )
    expect(versions).to(have_len(4))
    expect(versions[-1].is_delete_marker).to(be_true)

    # removing the object again will have no effect
    client.remove_object(bucket_name, object_name)
    versions2 = list(
        client.buckets[bucket_name].objects[object_name].versions.values()
    )
    expect(versions2).to(equal(versions))

    # putting a new version after deletion will add a new version
    client.fput_object(bucket_name, object_name, file_path)
    expect(
        list(
            client.list_objects(bucket_name, object_name, include_version=True)
        )
    ).to(have_len(5))

    expect(list(client.list_objects(bucket_name, object_name))).to(have_len(1))

    # removing the object again with versioning enabled will add a new deletion
    # marker
    client.remove_object(bucket_name, object_name)
    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )
    expect(objects).to(have_len(6))
    versions = list(
        client.buckets[bucket_name].objects[object_name].versions.values()
    )
    expect(versions).to(have_len(6))
    expect(versions[0].is_delete_marker).to(be_false)
    expect(versions[1].is_delete_marker).to(be_false)
    expect(versions[2].is_delete_marker).to(be_false)
    expect(versions[3].is_delete_marker).to(be_true)
    expect(versions[4].is_delete_marker).to(be_false)
    expect(versions[5].is_delete_marker).to(be_true)

    expect(objects[0].is_delete_marker).to(be_false)
    expect(objects[1].is_delete_marker).to(be_false)
    expect(objects[2].is_delete_marker).to(be_false)
    expect(objects[3].is_delete_marker).to(be_false)
    expect(objects[4].is_delete_marker).to(be_true)
    expect(objects[5].is_delete_marker).to(be_true)

    expect(objects[0].version_id).to(equal(versions[4].version_id))
    expect(objects[1].version_id).to(equal(versions[2].version_id))
    expect(objects[2].version_id).to(equal(versions[1].version_id))
    expect(objects[3].version_id).to(equal(versions[0].version_id))
    expect(objects[4].version_id).to(equal(versions[5].version_id))
    expect(objects[5].version_id).to(equal(versions[3].version_id))

    # trying to an object marked for deletion by version will raise an exception
    with pytest.raises(S3Error, match="not allowed against this resource"):
        client.get_object(
            bucket_name, object_name, version_id=versions[3].version_id
        )

    to_delete = [
        DeleteObject(object_name, objects[i].version_id)
        for i in range(len(objects) - 2)
    ]
    multiple_delete_result = client._delete_objects(bucket_name, to_delete)
    expect(multiple_delete_result).to(be_a(DeleteResult))
    expect(multiple_delete_result.error_list).to(have_len(0))
    expect(multiple_delete_result.object_list).to(have_len(4))

    for i, obj in enumerate(multiple_delete_result.object_list):
        expect(obj).to(be_a(DeletedObject))
        expect(obj.name).to(equal(object_name))
        expect(obj.version_id).to(equal(objects[i].version_id))
        expect(obj.delete_marker).to(be_false)
        expect(obj.delete_marker_version_id).to(be_none)

    to_delete = [
        DeleteObject(object_name, objects[i].version_id) for i in (4, 5)
    ]

    multiple_delete_result = client._delete_objects(bucket_name, to_delete)
    expect(multiple_delete_result.error_list).to(have_len(0))

    for i, obj in enumerate(multiple_delete_result.object_list):
        expect(obj).to(be_a(DeletedObject))
        expect(obj.name).to(equal(object_name))
        expect(obj.version_id).to(equal(objects[4 + i].version_id))
        expect(obj.delete_marker).to(be_false)
        expect(obj.delete_marker_version_id).to(be_none)

    version_id = client.fput_object(
        bucket_name, object_name, file_path
    ).version_id
    to_delete = [DeleteObject(object_name)]

    multiple_delete_result = client._delete_objects(bucket_name, to_delete)
    expect(multiple_delete_result.error_list).to(have_len(0))
    obj = multiple_delete_result.object_list[0]
    expect(obj).to(be_a(DeletedObject))
    expect(obj.name).to(equal(object_name))
    expect(obj.version_id).to(be_none)
    expect(obj.delete_marker).to(be_true)
    expect(obj.delete_marker_version_id).not_to(be_none)
    expect(obj.delete_marker_version_id).not_to(equal(version_id))


@pytest.mark.FUNC()
def test_versioned_objects_after_upload(minio_mock):
    bucket_name = "test-bucket"
    object_name = "test-object"
    file_path = "tests/fixtures/maya.jpeg"

    client = Minio("http://local.host:9000")
    client.make_bucket(bucket_name)
    client.fput_object(bucket_name, object_name, file_path)
    client.set_bucket_versioning(bucket_name, VersioningConfig(ENABLED))
    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )
    expect(objects).to(have_len(1))
    first_version = objects[0].version_id
    expect(first_version).to(be_none)

    client.fput_object(bucket_name, object_name, file_path)
    client.fput_object(bucket_name, object_name, file_path)
    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )
    last_version = objects[1].version_id
    expect(objects).to(have_len(3))
    expect(objects[-1].version_id).to(be_none)
    expect(last_version).not_to(be_none)
    client.set_bucket_versioning(bucket_name, VersioningConfig(SUSPENDED))

    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )

    client.remove_object(bucket_name, object_name, objects[0].version_id)
    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )
    expect(objects).to(have_len(2))

    client.remove_object(bucket_name, object_name)
    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )
    expect(objects).to(have_len(2))
    expect(objects[-1].is_delete_marker).to(be_true)

    client.remove_object(bucket_name, object_name, "null")
    objects = list(
        client.list_objects(bucket_name, object_name, include_version=True)
    )
    expect(objects).to(have_len(1))


def test_stat_object(minio_mock):
    bucket_name = "test-bucket"
    object_name = "test-object"

    client = Minio("http://local.host:9000")
    client.make_bucket(bucket_name)

    client.put_object(
        bucket_name,
        object_name,
        b"coucou",
        6,
    )
    stat = client.stat_object(bucket_name, object_name)
    expect(stat.bucket_name).to(equal(bucket_name))
    expect(stat.object_name).to(equal(object_name))
    expect(stat.size).to(equal(6))

    client.set_bucket_versioning(bucket_name, VersioningConfig(ENABLED))
    stat = client.stat_object(bucket_name, object_name)
    expect(stat.bucket_name).to(equal(bucket_name))
    expect(stat.object_name).to(equal(object_name))
    expect(stat.size).to(equal(6))

    client.put_object(
        bucket_name,
        object_name,
        b"coucouuu",
        8,
    )

    stat = client.stat_object(bucket_name, object_name)
    expect(stat.bucket_name).to(equal(bucket_name))
    expect(stat.object_name).to(equal(object_name))
    expect(stat.size).to(equal(8))

    stat = client.stat_object(bucket_name, object_name, version_id="null")
    expect(stat.bucket_name).to(equal(bucket_name))
    expect(stat.object_name).to(equal(object_name))
    expect(stat.size).to(equal(6))


@pytest.mark.FUNC()
@pytest.mark.parametrize("versioned", (True, False))
def test_file_download(minio_mock, versioned):
    bucket_name = "test-bucket"
    object_name = "test-object"
    file_content = b"Test file content"
    length = sys.getsizeof(file_content)
    client = Minio("http://local.host:9000")
    client.make_bucket(bucket_name)
    version = None
    if versioned:
        client.set_bucket_versioning(bucket_name, VersioningConfig(ENABLED))
    client.put_object(bucket_name, object_name, file_content, length)
    if versioned:
        version = next(
            client.list_objects(bucket_name, object_name, include_version=True)
        ).version_id

    response = client.get_object(bucket_name, object_name)
    expect(response.data).to(equal(file_content))
    if versioned:
        response = client.get_object(
            bucket_name, object_name, version_id=version
        )
        expect(response.data).to(equal(file_content))


def test_bucket_exists(minio_mock):
    bucket_name = "existing-bucket"
    client = Minio("http://local.host:9000")
    client.make_bucket(bucket_name)
    expect(client.bucket_exists(bucket_name)).to(be_true)


def test_bucket_versioning(minio_mock):
    bucket_name = "existing-bucket"
    client = Minio("http://local.host:9000")
    client.make_bucket(bucket_name)
    expect(client.get_bucket_versioning(bucket_name).status).to(equal("Off"))
    client.set_bucket_versioning(bucket_name, VersioningConfig(ENABLED))
    expect(client.get_bucket_versioning(bucket_name).status).to(
        equal("Enabled")
    )
    client.set_bucket_versioning(bucket_name, VersioningConfig("Suspended"))
    expect(client.get_bucket_versioning(bucket_name).status).to(
        equal("Suspended")
    )


@pytest.mark.parametrize("versioned", (True, False))
def test_get_presigned_url(minio_mock, versioned):
    bucket_name = "test-bucket"
    object_name = "test-object"
    file_path = "tests/fixtures/maya.jpeg"

    client = Minio("http://local.host:9000")
    client.make_bucket(bucket_name)
    version = None
    if versioned:
        client.set_bucket_versioning(bucket_name, VersioningConfig(ENABLED))
    client.fput_object(bucket_name, object_name, file_path)
    if versioned:
        version = list(
            client.list_objects(bucket_name, object_name, include_version=True)
        )[-1].version_id
    url = client.get_presigned_url(
        "GET", bucket_name, object_name, version_id=version
    )
    expect(validators.url(url)).to(be_true)
    if version:
        expect(url.endswith(f"?versionId={version}")).to(be_true)


def test_presigned_put_url(minio_mock):
    bucket_name = "test-bucket"
    object_name = "test-object"
    file_path = "tests/fixtures/maya.jpeg"

    client = Minio("http://local.host:9000")
    client.make_bucket(bucket_name)
    client.fput_object(bucket_name, object_name, file_path)
    url = client.presigned_put_object(bucket_name, object_name)
    expect(validators.url(url)).to(be_true)


def test_presigned_get_url(minio_mock):
    bucket_name = "test-bucket"
    object_name = "test-object"
    file_path = "tests/fixtures/maya.jpeg"

    client = Minio("http://local.host:9000")
    client.make_bucket(bucket_name)
    client.fput_object(bucket_name, object_name, file_path)
    url = client.presigned_get_object(bucket_name, object_name)
    expect(validators.url(url)).to(be_true)


def test_list_buckets(minio_mock):
    client = Minio("http://local.host:9000")
    buckets = client.list_buckets()
    n = len(buckets)
    bucket_name = "new-bucket"
    client.make_bucket(bucket_name)
    buckets = client.list_buckets()
    expect(buckets).to(have_len(n + 1))


def test_list_objects(minio_mock):
    client = Minio("http://local.host:9000")

    with pytest.raises(S3Error):
        _ = client.list_objects("no-such-bucket")

    bucket_name = "new-bucket"
    client.make_bucket(bucket_name)
    objects = client.list_objects(bucket_name)
    expect(list(objects)).to(have_len(0))

    client.put_object(
        bucket_name, "a/b/c/object1", data=b"object1 data", length=12
    )
    client.put_object(
        bucket_name, "a/b/object2", data=b"object2 data", length=12
    )
    client.put_object(bucket_name, "a/object3", data=b"object3 data", length=11)
    client.put_object(bucket_name, "object4", data=b"object4 data", length=11)

    # Test recursive listing
    objects_recursive = list(
        client.list_objects(bucket_name, prefix="a/", recursive=True)
    )
    expect(objects_recursive).to(have_len(3))
    expect({obj.object_name for obj in objects_recursive}).to(
        equal(
            {
                "a/b/c/object1",
                "a/b/object2",
                "a/object3",
            }
        )
    )

    # Test non-recursive listing
    objects_non_recursive = client.list_objects(
        bucket_name, prefix="a/", recursive=False
    )

    expect({obj.object_name for obj in objects_non_recursive}).to(
        equal(
            {
                "a/object3",
                "a/b/",
            }
        )
    )

    # Test listing at the bucket root
    objects_root = client.list_objects(bucket_name, recursive=False)
    # Check that the correct paths are returned

    expect({obj.object_name for obj in objects_root}).to(
        equal({"a/", "object4"})
    )


def test_connecting_to_the_same_endpoint(minio_mock):
    client_1 = Minio("http://local.host:9000")
    client_1_buckets = ["bucket-1", "bucket-2", "bucket-3"]
    for bucket in client_1_buckets:
        client_1.make_bucket(bucket)

    client_2 = Minio("http://local.host:9000")
    client_2_buckets = client_2.list_buckets()
    expect(client_2_buckets).to(equal(client_1_buckets))


def test_compose(minio_mock):
    client = Minio("http://local.host:9000")
    bucket_name = "new-bucket"
    client.make_bucket(bucket_name)
    client.put_object(bucket_name, "test.txt", b"hello", 5, metadata={"a": "A"})
    client.put_object(
        bucket_name, "test2.txt", b" world", 6, metadata={"b": "B"}
    )
    res = client.compose_object(
        bucket_name,
        "test3.txt",
        [
            ComposeSource(bucket_name, "test.txt"),
            ComposeSource(bucket_name, "test2.txt"),
        ],
        metadata={"a": "C"},
    )
    expect(res).to(be_a(ObjectWriteResult))
    data = client.get_object(bucket_name, "test3.txt").data
    expect(data).to(equal(b"hello world"))
    expect(client.stat_object(bucket_name, "test3.txt").metadata).to(
        equal({"a": "C", "b": "B"})
    )


def test_copy(minio_mock):
    client = Minio("http://local.host:9000")
    bucket_name = "new-bucket"
    client.make_bucket(bucket_name)
    client.put_object(bucket_name, "test.txt", b"hello", 5, metadata={"a": "A"})
    res = client.copy_object(
        bucket_name,
        "test2.txt",
        CopySource(bucket_name, "test.txt"),
        metadata={"a": "C"},
    )
    expect(res).to(be_a(ObjectWriteResult))
    data = client.get_object(bucket_name, "test2.txt").data
    expect(data).to(equal(b"hello"))
    expect(client.stat_object(bucket_name, "test2.txt").metadata).to(
        equal({"a": "C"})
    )
