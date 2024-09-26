import sys
from datetime import datetime
from typing import Any, Generator

import pymongo


def find_intersections(
    client: pymongo.MongoClient,
    db_name: str,
    collection_name: str,
    chromosome: str,
    start: int,
    end: int,
    inside_only: bool = False,
    attributes: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Find intersecting items within a single collection using indexed queries.
    :param client: pymongo.MongoClient - MongoDB client
    :param db_name: str - name of the database
    :param collection_name: str - the name of the collection to search
    :param chromosome: str - the chromosome identifier
    :param start: int - the start of the interval
    :param end: int - the end of the interval
    :param inside_only: bool - whether to only include items completely within the interval
    :param attributes: list[str] or None - list of attributes to include in the result, or None to include all
    :return: list[dict[str, Any]] - a list of dictionaries containing the matching items
    """
    # Use indexed query to find potential intersections
    if inside_only:
        # Inside only: items completely within the given interval
        query = {"chromosome": chromosome, "start": {"$gte": start}, "end": {"$lte": end}}
    else:
        # General overlap: any items that overlap the given interval
        query = {
            "chromosome": chromosome,
            "start": {"$lte": end},  # search_params.start <= other.end
            "end": {"$gte": start},  # search_params.end >= other.start
        }

    # Prepare projection if attributes are specified
    projection = {attr: 1 for attr in attributes} if attributes else None

    db = client[db_name]
    collection = db[collection_name]
    return list(collection.find(query, projection))


def _insert_into_collection(
    db_col: pymongo.collection.Collection, data_generator: Generator[dict, None, None], batch_insert: int = 100
) -> int:
    """
    Insert data into MongoDB collection from a generator.
    :param db_col: pymongo.collection.Collection - MongoDB collection
    :param data_generator: generator - generator yielding data to insert
    :param batch_insert: int - how many items to batch insert
    :return: int - number of inserted items
    """
    items_insert = []
    num_items = 0

    for i, item in enumerate(data_generator):
        items_insert.append(item)
        num_items += 1

        if len(items_insert) >= batch_insert:
            db_col.insert_many(items_insert)
            items_insert.clear()

        # verbose output
        if i % 10000 == 0:
            print(f"{db_col.name:<20}: Filling table {i:>8} items inserted", file=sys.stderr)

    # insert last batch
    if len(items_insert) > 0:
        db_col.insert_many(items_insert)

    return num_items


def insert_into_mongodb(
    client: pymongo.MongoClient,
    db_name: str,
    collection_name: str,
    data_generator: Generator[dict, None, None],
    has_cnv_type: bool = False,
    more_indexes: list[str] | None = None,
    batch_insert: int = 100,
) -> int:
    """
    Inserts data from a generator into a MongoDB collection, logging the process.
    :param client: pymongo.MongoClient - MongoDB client
    :param db_name: str - name of the database
    :param collection_name: str - name of the collection to insert data into
    :param data_generator: generator - generator yielding data to insert
    :param has_cnv_type: bool - does the collection have cnv_type?
    :param more_indexes: list[str] - create some more indexes?
    :param batch_insert: int - how many items to batch insert
    :return: int - number of inserted items
    """
    try:
        start_time = datetime.now()
        print(f"Filling Mongo    : {start_time.isoformat()}", file=sys.stderr)

        # create client and collection
        print("Fetching the client", file=sys.stderr)
        db = client[db_name]

        # delete previous table
        if collection_name in db.list_collection_names():
            print(f"Deleting {collection_name} collection", file=sys.stderr)
            db[collection_name].drop()

        # create new collection
        collection = db[collection_name]

        # create indices
        print("Creating indices", file=sys.stderr)
        collection.create_index(
            [("cnv_type", pymongo.ASCENDING)]
            if has_cnv_type
            else [] + [("chromosome", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]
        )
        collection.create_index(
            [("cnv_type", pymongo.ASCENDING)]
            if has_cnv_type
            else [] + [("chromosome", pymongo.ASCENDING), ("end", pymongo.ASCENDING)]
        )
        if more_indexes is not None:
            for index in more_indexes:
                collection.create_index([(index, pymongo.ASCENDING)])

        # fill collections
        print("Filling collection", file=sys.stderr)
        items_cnvs = _insert_into_collection(collection, data_generator, batch_insert)
        print(str(items_cnvs), file=sys.stderr)

        # end
        end_time = datetime.now()
        print(f"Finished Mongo   : {end_time.isoformat()}", file=sys.stderr)
        print(f"Time of run      : {end_time - start_time}", file=sys.stderr)

    except pymongo.errors.ServerSelectionTimeoutError:
        raise OSError("Could not connect to a running MongoDB database - please ensure that the database is running!")

    return items_cnvs
