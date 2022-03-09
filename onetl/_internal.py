from __future__ import annotations


def uniq_ignore_case(orig_list: list[str]) -> list[str]:
    """
    Return only uniq values from a list, case ignore.

    Examples
    --------

    .. code:: python

        assert uniq_ignore_case(["a", "c"]) == ["a", "c"]
        assert uniq_ignore_case(["a", "a", "c"]) == ["a", "c"]
        assert uniq_ignore_case(["a", "A", "c"]) == ["a", "c"]
    """

    result: list[str] = []
    result_lower: list[str] = []

    for orig_value in orig_list:
        if orig_value.lower() not in result_lower:
            result.append(orig_value)
            result_lower.append(orig_value.lower())

    return result
