from minio.error import S3Error


def no_such_bucket(bucket_name):
    return S3Error(
        message="bucket does not exist",
        resource=f"/{bucket_name}",
        request_id=None,
        host_id=None,
        response="mocked_response",
        code=404,
        bucket_name=bucket_name,
        object_name=None,
    )


def no_such_key(bucket_name, object_name):
    return S3Error(
        message="The specified key does not exist.",
        resource=f"/{bucket_name}/{object_name}",
        request_id=None,
        host_id=None,
        response="mocked_response",
        code=404,
        bucket_name=bucket_name,
        object_name=object_name,
    )


def invalid_version(bucket_name, object_name):
    return S3Error(
        message="Invalid version id specified",
        resource=f"/{bucket_name}/{object_name}",
        request_id=None,
        host_id=None,
        response="mocked_response",
        code=422,
        bucket_name=bucket_name,
        object_name=object_name,
    )


def no_such_version(bucket_name, object_name):
    return S3Error(
        message="The specified version does not exist",
        resource=f"/{bucket_name}/{object_name}",
        request_id=None,
        host_id=None,
        response="mocked_response",
        code=404,
        bucket_name=bucket_name,
        object_name=object_name,
    )


def method_not_allowed(bucket_name, object_name):
    return S3Error(
        message="The specified method is not allowed against this resource.",
        resource=f"/{bucket_name}/{object_name}",
        request_id=None,
        host_id=None,
        response="mocked_response",
        code=403,
        bucket_name=bucket_name,
        object_name=object_name,
    )