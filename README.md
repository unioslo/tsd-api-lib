
# tsd-api-lib

Tools to build the TSD API, and its clients. Currently includes:

* Simple, efficient content verification of files and directories of files, with configurable caching, based on [blake3](https://github.com/BLAKE3-team/BLAKE3).

## Usage

Basic usage:
```python
from tsdapilib.verifier import ContentVerifier

cv = ContentVerifier()
cv.check_file("/my/file")
```

Performing incremental hashing, with a persistent cache, which can be shared across processes:
```python
from tsdapilib.verifier import ContentVerifier
from tsdapilib.backends import PostgresBackend

postgres_config = {
    "dbname": "some-db",
    "user": "some-user",
    "pw": "some-pw",
    "host": "some-host",
}
cv = ContentVerifier(backend=PostgresBackend(postgres_config))

cv.start("large-file")
with cv._lazy_read("large-file", chunk_size=1024) as f:
    # serve the chunk over the network
    # do something else with it
    cv.update("large-file", chunk)
hash_value = cv.finish("large-file")
# communicte the hash_value
```

The library also provides a `SQLiteBackend` persistent cache as a more lightweight alternative to the `PostgresBackend`.

## Testing

To run the tests, do:
```bash
pip3 install pytest
pytest tsdapilib/tests.py
```

## License

BSD.
