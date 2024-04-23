# pytest-minio-mock

Forked from https://github.com/oussjarrousse/pytest-minio-mock

## Overview
`pytest-minio-mock` is a pytest plugin designed to simplify testing of applications that interact with Minio the code  S3 compatible object storage system. It is not designed to test the correnction to the minio server. It provides a fixture that mocks the `minio.Minio` class, allowing for easy testing of Minio interactions without the need for a real Minio server.

The plugin supports python version 3.11 or above.

## Features
- Mock implementation of the `minio.Minio` client.
- Easy to use pytest fixture.
- Supports common Minio operations such as bucket creation, file upload/download, etc.
- Supports file versioning

## Installation

To install `pytest-minio-mock`, run:

```bash
pip install git+https://github.com/cottephi/pytest-minio-mock.git
```

## Usage
To use the minio_mock fixture in your pytest tests, simply include it as a parameter in your test functions. Then use minio.Minio() as usual.

The mocker will create one instance per provided endpoint.

Here's an example:

```python
import minio
import logging
from pytest_minio_mock import minio_mock_servers, minio_mock  # Need to import both 

def foo():
    try:
        minio_client = minio.Minio(
            endpoint="http://dummy",
            access_key="useless",  # unused in mocks
            secret_key="useless",  # unused in mocks
            region="useless",  # unused in mocks
        )
        return minio_client.make_bucket("buckets")
    except Exception as e:
        logging.error(e)


def test_file_upload(minio_mock):
    # Calling function foo that involves using minio.Minio()
    assert foo()
    minio_client = minio.Minio(
            endpoint="http://dummy",
            access_key="useless",
            secret_key="useless",
            region="useless"
        )
    buckets = minio_client.list_buckets()
    assert len(buckets)==1
```

The `minio_mock` fixture will patch newly created minio.Minio() thus providing you with a way to test your code around the Minio client easily.

At the moment, instances of minio.Minio() created before loading the minio_mock fixture code will not be patched.

This might be an issue if one or more of the fixtures you are using in your tests that preceeds `minio_mock` 
in the parameters list of the test function, initiates instances of minio.Minio() that you want to test.
As a workaround make sure that minio_mock is the first fixture in the list of arguments of function where minio_mock is needed. Example:

```python
import pytest

@pytest.fixture()
def system_under_test(minio_mock, some_other_fixture):
    # your code here
    pass
```

## License
pytest-minio-mock is licensed under the MIT License - see the LICENSE file for details.
