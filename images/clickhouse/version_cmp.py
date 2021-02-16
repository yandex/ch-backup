# Simple script that compares two given ClickHouse versions in format (XX.YY.ZZ)
# If first given version is equal or greater than second given version
# script prints 1 to standart output or prints 0 in other case.

import sys
from typing import List


def split(ver: str):
    return list(map(lambda x: int(x), ver.split('.')))


# Returns 1 if ver1 >= ver2 and 0 in other case.
def cmp(ver1: List[int], ver2: List[int]):
    for i in range(min(len(ver1), len(ver2))):
        if ver1[i] < ver2[i]:
            return 0
        elif ver1[i] > ver2[i]:
            return 1

    return 1


if __name__ == "__main__":
    # Should be exactly 2 arguments.
    if len(sys.argv) - 1 != 2:
        sys.exit(1)

    ver1 = sys.argv[1]
    ver2 = sys.argv[2]
    print(cmp(split(ver1), split(ver2)))
