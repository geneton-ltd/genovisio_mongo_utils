# genovisio_mongo_utils

This package contains common functionality shared by genovisio workflow to build genovisio database

## Usage

Install like any other pip package from git:

```sh
pip install git+https://github.com/geneton-ltd/genovisio_mongo_utils.git@{TAG}
```

or add it similarly as pip dependency in conda. TAG represents the release version, for example `v0.1.0`

Then, use it as any other python library in python scripts:

```py
import genovisio_utils

genovisio_utils.insert_into_mongodb(
    client=client,
    db_name=db_name,
    collection_name=collection_name,
    data_generator=data_generator,
)
```
