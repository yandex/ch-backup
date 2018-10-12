"""
Variables that influence testing behavior are defined here.
"""
import string
from random import choice as random_choise


def generate_random_string(length=64):
    """
    Generate random alphanum sequence
    """
    return ''.join(
        random_choise(string.ascii_letters + string.digits)
        for _ in range(length))
