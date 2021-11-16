"""
Testing utilities.
"""
from typing import List

import pytest


def parametrize(*tests):
    """
    A wrapper for `pytest.mark.parametrize` that eliminates parallel lists in the interface.

    Example:
    ```
    @parametrize(
        {
            'id': 'test1',
            'args': {
                'arg1': 'value1-1',
                'arg2': 'value1-2',
            }
        },
        {
            'id': 'test2',
            'args': {
                'arg1': 'value2-1',
                'arg2': 'value2-2',
            }
        }
    )
    ```
    It equals to:
    ```
    @pytest.mark.parametrize(
        ids=['test1', 'test2'],
        argnames=['arg1', 'arg2'],
        argvalues=[
            (
                'value1-1',
                'value1-2',
            ),
            (
                'value2-1',
                'value2-2',
            ),
        ]
    )
    ```
    """
    ids: List[str] = []
    argnames: List[str] = []
    argvalues: list = []
    for test in tests:
        ids.append(test['id'])

        test_args = sorted(test['args'].items())
        test_argnames = [arg[0] for arg in test_args]
        test_argvalues = [arg[1] for arg in test_args]

        if not argnames:
            argnames = test_argnames
        else:
            assert argnames == test_argnames

        argvalues.append(test_argvalues)

    return pytest.mark.parametrize(ids=ids, argnames=argnames, argvalues=argvalues)
