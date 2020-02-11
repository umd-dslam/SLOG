#!/usr/bin/python3
from __future__ import (
    division, 
    print_function
)

import base64
import logging
import numpy as np
import os
import string
import time

from argparse import ArgumentParser
from collections import defaultdict
from functools import partial

from google.protobuf.internal.encoder import _VarintBytes
from multiprocessing import Pool

from fnv_hash import fnv_hash
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
FILE_EXTENSION = '.dat'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s: %(message)s'
)
LOG = logging.getLogger("gen_data")
LOG_EVERY_SEC = 1

def encode_key(key: int) -> bytes:
    """
    Encodes an integer key into a fixed length string
    """
    return base64.b64encode(
        key.to_bytes(8, byteorder='little')
    )


class DataGenerator:

    def __init__(
        self,
        data_dir: str,
        prefix: str,
        num_replicas: int,
        num_partitions: int,
        size: int,
        size_unit: str,
        record_size: int,
        max_jobs: int,
    ):
        self.data_dir = os.path.abspath(data_dir)
        self.prefix = prefix
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        total_size = MULTIPLIERS[size_unit] * size
        num_records = total_size / (KEY_SIZE + record_size + MASTER_SIZE)
        self.num_records = int(num_records)
        self.record_size = record_size
        self.max_jobs = max_jobs

    def partition_of_key(self, key: int) -> int:
        return fnv_hash(encode_key(key)) % self.num_partitions

    def gen_data(self, as_text: bool) -> None:
        start_time = time.time()

        LOG.info(
            "Creating %d partitions from %d keys...", 
            self.num_partitions, self.num_records
        )
        # Distribute keys into the partitions
        partition_to_keys = defaultdict(list)
        for key in range(0, self.num_records):
            p = self.partition_of_key(key)
            partition_to_keys[p].append(key)

        # Compute the number of jobs
        num_jobs = (
            min(self.num_partitions, self.max_jobs)
            if self.max_jobs > 0 else self.num_partitions
        )
        LOG.info("Spawning %d jobs...", num_jobs)
        func = partial(
            DataGenerator.gen_data_per_partition,
            as_text=as_text,
            obj=self,
        )
        with Pool(num_jobs) as pool:
            # Map func(partition, key_list) to each item in the key map
            pool.starmap(func, partition_to_keys.items())

        LOG.info("Done. Elapsed time: %.2f seconds", time.time() - start_time)
    
    @staticmethod
    def gen_data_per_partition(
        partition: int,
        keys: list,
        as_text: bool,
        obj: object,
    ):
        '''
        Wrapper for the class method __gen_data_per_partition so that it can be
        used in multiprocessing.
        '''
        obj.__gen_data_per_partition(partition, keys, as_text)

    def __gen_data_per_partition(
        self,
        partition: int,
        keys: list,
        as_text: bool,
    ) -> None:
        file_name = os.path.join(
            self.data_dir,
            self.prefix + str(partition) + FILE_EXTENSION
        )
        LOG.info("Generating data for %s", file_name)
        mode = 'w' if as_text else 'wb'
        part_file = open(file_name, mode)

        # Write number of keys in this partition
        if as_text:
            part_file.write(str(len(keys)) + "\n")
        else:
            part_file.write(_VarintBytes(len(keys)))

        last_time = time.time()
        last_index = 0
        for i, key in enumerate(keys):
            # Generate the datum for this key
            datum = self.__gen_datum(key, as_text)
            # Append the datum to file
            part_file.write(datum)

            now = time.time()
            if now - last_time >= LOG_EVERY_SEC:
                pct = (i) / len(keys) * 100
                rate = (i - last_index) / LOG_EVERY_SEC
                LOG.info(
                    "Progress: %d/%d (%.1f%%). Rate: %d datums/s",
                    i + 1,
                    len(keys),
                    pct,
                    rate)
                last_time = now
                last_index = i

        part_file.close()

    def __gen_datum(self, key: int, as_text=False):
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
            # Size of the datum and the serialized datum
            datum = (
                _VarintBytes(datum_proto.ByteSize()) +
                datum_proto.SerializeToString()
            )
        
        return datum

# TODO(ctring): Take into account number of bytes actually used for partitioning
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
        help="Generate data as human-readable text files"
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=0,
        help="Maximum number of jobs spawned to do work. For unlimited number "
             "of jobs, use 0."
    )

    args = parser.parse_args()

    # Keys and master are not randomly generated
    # so this seed only affects record
    np.random.seed(0)

    DataGenerator(
        args.data_dir,
        "",
        args.num_replicas,
        args.num_partitions,
        args.size,
        args.size_unit,
        args.record_size,
        args.max_jobs,
    ).gen_data(
        as_text=args.as_text,
    )