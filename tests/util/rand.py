from random import randint
from string import ascii_lowercase


def rand_str(alphabet: str = ascii_lowercase, length: int = 10) -> str:
    alphabet_length = len(alphabet)
    return "".join(alphabet[randint(0, alphabet_length - 1)] for _ in range(length))  # noqa: S311
