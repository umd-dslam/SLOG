import sys
if sys.version_info[0] == 3:
    _get_byte = lambda c: c
else:
    _get_byte = ord

from argparse import ArgumentParser


def fnv_hash(value: bytes) -> int:
    assert isinstance(value, bytes)

    FNV_32_PRIME = 0x01000193
    FNV_32_SIZE = 2**32

    hash = 0x811c9dc5
    for byte in value:
        hash = (hash * FNV_32_PRIME) % FNV_32_SIZE
        hash = hash ^ _get_byte(byte)
    return hash


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Computes FNV hash for a given string"
    )
    parser.add_argument("string", help="A string to compute FNV hash")
    parser.add_argument(
        "-m",
        type=int,
        help="The value to take modulo of the result by"
    )
    args = parser.parse_args()
    result = fnv_hash(args.string.encode())
    if args.m is not None:
        result %= args.m
    print(result)