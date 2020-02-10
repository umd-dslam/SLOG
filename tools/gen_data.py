from __future__ import (
    division, 
    print_function
)

import base64
import numpy as np
import os
import string
import sys
if sys.version_info[0] == 3:
    _get_byte = lambda c: c
else:
    _get_byte = ord

from argparse import ArgumentParser
from collections import defaultdict
from google.protobuf.internal.encoder import _VarintBytes
from proto.offline_data_pb2 import Datum

ALPHABET = np.array(list(string.ascii_lowercase + string.digits))
MULTIPLIERS = {
    "b" : 1024 ** 0,
    "kb": 1024 ** 1,
    "mb": 1024 ** 2,
    "gb": 1024 ** 3,
}

KEY_SIZE = 12   # 8 bytes + overhead from base64 encoding
MASTER_SIZE = 2


def encode_key(key: int) -> bytes:
    """
    Encodes an integer key into a fixed length string
    """
    return base64.b64encode(
        key.to_bytes(8, byteorder='little')
    )


def fnv_hash(value: bytes) -> int:
    assert isinstance(value, bytes)

    FNV_32_PRIME = 0x01000193
    FNV_32_SIZE = 2**32

    hash = 0x811c9dc5
    for byte in value:
        hash = (hash * FNV_32_PRIME) % FNV_32_SIZE
        hash = hash ^ _get_byte(byte)
    return hash


class DataGenerator:

    def __init__(
        self,
        num_replicas: int,
        num_partitions: int,
        size: int,
        size_unit: str,
        record_size: int,
    ):
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        total_size = MULTIPLIERS[size_unit] * size
        num_records = total_size / (KEY_SIZE + record_size + MASTER_SIZE)
        self.num_records = int(num_records)
        self.record_size = record_size

    def key_to_partition(self, key: int) -> int:
        return fnv_hash(encode_key(key)) % self.num_partitions

    def gen_datum(self, key: int, as_text=False):
        encoded_key = encode_key(key)
        record = ''.join(
            np.random.choice(
                ALPHABET,
                size=self.record_size
            )
        )
        master = key % self.num_replicas

        if as_text:
            datum_tuple = map(str, (encoded_key.decode(), record, master))
            datum = ','.join(datum_tuple) + '\n'
        else:
            datum_proto = Datum()
            datum_proto.key = encoded_key.decode()
            datum_proto.record = record
            datum_proto.master = master
            datum = (
                _VarintBytes(datum_proto.ByteSize()) +
                datum_proto.SerializeToString()
            )
        
        return datum

    def gen_data(self, data_dir: str, prefix='', as_text=False) -> None:
        # Open a file for each partition
        file_names = [
            os.path.join(data_dir, prefix + str(p) + '.dat')
            for p in range(self.num_partitions)
        ]
        mode = 'w' if as_text else 'wb'
        part_files = [open(f, mode) for f in file_names]


        # Count the number of keys for each partition and write to the
        # beginning of its data file
        key_counts = defaultdict(int)
        for key in range(0, self.num_records):
            p = self.key_to_partition(key)
            key_counts[p] += 1

        for p in key_counts:
            c = key_counts[p]
            if as_text:
                part_files[p].write(str(c) + "\n")
            else:
                part_files[p].write(_VarintBytes(c))


        # Generate the actual data
        for key in range(0, self.num_records):
            partition = self.key_to_partition(key)
            datum = self.gen_datum(key, as_text)
            part_files[partition].write(datum)


        # Close the files
        for p in part_files:
            p.close()


if __name__ == "__main__":
    parser = ArgumentParser(
        "gen_data",
        description="Generate initial data for SLOG"
    )
    parser.add_argument(
        "data_dir",
        help="Directory where the generated data files are located",
    )
    parser.add_argument(
        "-p", "--num-partitions",
        default=1,
        type=int,
        help="Number of partitions"
    )
    parser.add_argument(
        "-r", "--num_replicas",
        default=1,
        type=int,
        help="Number of replicas"
    )
    parser.add_argument(
        "-s", "--size",
        default=1,
        type=int,
        help="Total size of the generated data across all partitions")
    parser.add_argument(
        "-su", "--size-unit",
        choices=['gb', 'mb', 'kb', 'b'],
        default="mb",
        type=str.lower,
        help="Unit for the option --size"
    )
    parser.add_argument(
        "-rs", "--record-size",
        default=100,
        type=int,
        help="Size of each record, in bytes."
    )
    parser.add_argument(
        "--as-text",
        action='store_true',
        help="Generate data as text files"
    )

    args = parser.parse_args()

    np.random.seed(0)

    DataGenerator(
        args.num_replicas,
        args.num_partitions,
        args.size,
        args.size_unit,
        args.record_size,
    ).gen_data(
        args.data_dir,
        as_text=args.as_text,
    )