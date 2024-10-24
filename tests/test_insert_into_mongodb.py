from typing import Generator

import pymongo

import genovisio_utils


def test_generator() -> Generator[dict, None, None]:
    for i in range(10):
        yield {"chromosome": "chr1", "start": i, "end": i + 1}


def test_insert_into_mongodb(
    client: pymongo.MongoClient,
    db_name: str = "test_db",
    collection_name: str = "test_collection",
    data_generator: Generator[dict, None, None] = test_generator(),
):
    nums = genovisio_utils.insert_into_mongodb(
        client=client,
        db_name=db_name,
        collection_name=collection_name,
        data_generator=data_generator,
    )

    # using raise instead of assert to skip pytest dependency
    if nums != 10:
        raise ValueError(f"Expected 10, got {nums}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo_uri", type=str, required=True)
    args = parser.parse_args()

    client = pymongo.MongoClient(args.mongo_uri)
    test_insert_into_mongodb(client)
    print("insert_into_mongodb passed")
