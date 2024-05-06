from typing import Union


def _list_objects_checks(
    use_api_v1: bool, start_after: Union[str, None], delimiter: Union[str, None]
) -> tuple[str, bool]:
    if use_api_v1:
        raise ValueError("API V1 is not mocked")
    _start_after = start_after
    if _start_after is None:
        _start_after = ""
    _recursive = delimiter is None
    return _start_after, _recursive
