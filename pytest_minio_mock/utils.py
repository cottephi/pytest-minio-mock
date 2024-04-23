def _list_objects_checks(
    use_api_v1: bool, start_after: str | None, delimiter: str | None
) -> tuple[str, bool]:
    if use_api_v1:
        raise ValueError("API V1 is not mocked")
    _start_after = start_after
    if _start_after is None:
        _start_after = ""
    if delimiter is None:
        _recursive = True
    elif delimiter == "/":
        _recursive = False
    else:
        raise ValueError("Delimiter different from None or '/' is not mocked")
    return _start_after, _recursive
