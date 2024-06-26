from minio.error import S3Error
from urllib3.response import HTTPResponse


def no_such_bucket(bucket_name):
    return S3Error(
        message="bucket does not exist.",
        resource=f"/{bucket_name}",
        request_id=None,
        host_id=None,
        response=HTTPResponse("mocked_response"),
        code="NoSuchBucket",
        bucket_name=bucket_name,
        object_name=None,
    )


def no_such_key(bucket_name, object_name, is_deleted=False):
    return S3Error(
        message="Object does not exist",
        resource=f"/{bucket_name}/{object_name}",
        request_id=None,
        host_id=None,
        response=HTTPResponse(
            "mocked_response",
            headers={} if not is_deleted else {"x-amz-delete-marker": "true"},
        ),
        code="NoSuchKey",
        bucket_name=bucket_name,
        object_name=object_name,
    )


def invalid_version(bucket_name, object_name):
    return S3Error(
        message="Invalid version id specified.",
        resource=f"/{bucket_name}/{object_name}",
        request_id=None,
        host_id=None,
        response=HTTPResponse("mocked_response"),
        code="InvalidArgument",
        bucket_name=bucket_name,
        object_name=object_name,
    )


def no_such_version(bucket_name, object_name):
    return S3Error(
        message="The specified version does not exist.",
        resource=f"/{bucket_name}/{object_name}",
        request_id=None,
        host_id=None,
        response=HTTPResponse("mocked_response"),
        code="NoSuchVersion",
        bucket_name=bucket_name,
        object_name=object_name,
    )


def method_not_allowed(bucket_name, object_name):
    return S3Error(
        message="The specified method is not allowed against this resource.",
        resource=f"/{bucket_name}/{object_name}",
        request_id=None,
        host_id=None,
        response=HTTPResponse("mocked_response"),
        code="MethodNotAllowed",
        bucket_name=bucket_name,
        object_name=object_name,
    )
