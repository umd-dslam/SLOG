#!/usr/bin/python3
import sys
if sys.version_info[0] == 3:
    _get_byte = lambda c: c
else:
    _get_byte = ord

from argparse import ArgumentParser


def fnv_hash(value: bytes, num_bytes: int) -> int:
    assert isinstance(value, bytes)

    if num_bytes <= 0: num_bytes = len(value)

    FNV_32_PRIME = 0x01000193
    FNV_32_SIZE = 2**32

    hash = 0x811c9dc5
    for byte in value[:num_bytes]:
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
        help="The result will be taken modulo by this value."
    )
    parser.add_argument(
        "-b",
        type=int,
        default=0,
        help="Number of prefix bytes used to compute the hash. "
             "Set to 0 (default) to use the whole value."
    )
    args = parser.parse_args()
    result = fnv_hash(args.string.encode(), args.b)
    if args.m is not None:
        result %= args.m
    print(result)