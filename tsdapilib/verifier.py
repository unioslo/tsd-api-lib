
import os

from typing import Iterable, Optional

import blake3

from tsdapilib.backends import GenericBackend, EphemeralBackend
from tsdapilib.exc import (
    ContentVerificationReadError,
    ContentVerificationMissingReferenceError,
    ContentVerificationReferenceTypeError,
    ContentVerificationMissingHashError,
)


class ContentVerifier(object):

    """
    Cached blake3-based content verification of files.

    Examples:

    1. Simple usage on files and directories

    cv = ContentVerifier(backend=PostgreSQLBackend())
    cv.check_file('/path/to/file') # tries cache first
    cv.check_file('/path/to/file', force=True) # ignores cache, updates it
    cv.check_directory('/tmp')

    2. Incremental hashing (useful for stream processing)

    cv.start('/path/to/file') # adds the key, inits the hash func
    cv.start('/path/to/file', consume=1205) # bytes, to resume
    cv.update('/path/to/another/file', open(path, 'b').read()[1:offset])
    cv.update('/path/to/another/file', open(path, 'b').read()[offset:])
    cv.finish('/path/to/another/file') # returns the hexdigest

    References:

    https://github.com/BLAKE3-team/BLAKE3

    """

    def __init__(
        self,
        backend: GenericBackend = EphemeralBackend(),
    ) -> None:
        self._backend = backend
        self._buffer = {}

    @property
    def backend(self) -> GenericBackend:
        return self._backend

    @property
    def buffer(self) -> dict:
        return self._buffer

    def _lazy_read(
        self,
        reference: str,
        num_bytes: Optional[int] = None,
        chunk_size: int = 4096,
    ) -> Iterable:
        """
        Read a file, chunk-by-chunk, into an iterable of bytes.
        If provided, read only until num_bytes, truncating the
        last chunk size as necessary.

        """
        with open(reference, 'rb') as f:
            while True:
                if num_bytes:
                    bytes_read = f.tell()
                    if bytes_read > num_bytes:
                        raise ContentVerificationReadError
                    elif bytes_read == num_bytes:
                        break
                    elif bytes_read + chunk_size > num_bytes:
                        # prevent falling into the first branch
                        chunk_size = num_bytes - bytes_read
                data = f.read(chunk_size)
                if not data:
                    break
                else:
                    yield data

    def _consume_reference(
        self,
        reference: str,
        num_bytes: Optional[int] = None,
    ) -> blake3.blake3:
        """
        Consume a given amount of bytes from a file,
        in a memory efficient way, returning the hash
        object.

        """
        b3 = blake3.blake3()
        for data in self._lazy_read(reference, num_bytes):
            b3.update(data)
        return b3

    def _check_path(self, reference: str) -> str:
        """
        Ensure the content reference is a valid file path.

        """
        if not os.path.lexists(reference):
            raise ContentVerificationMissingReferenceError
        if os.path.isdir(reference):
            raise ContentVerificationReferenceTypeError
        return reference

    # public methods

    def check_file(self, reference: str, force: bool = False) -> Optional[str]:
        """
        Return the blake3 hexdigest of a file - either from the cache,
        or if that was stale or missing, update the cache with a fresh
        hash, and return it.

        """
        reference = self._check_path(reference)
        cached = self.backend.fetch(reference)
        if cached and not force:
            return cached.get('content_hash')
        else:
            content_hash = self._consume_reference(reference).hexdigest()
            self.backend.store(reference, content_hash)
            return content_hash

    def check_directory(self, reference: str, force: bool = False) -> Optional[dict]:
        """
        Scan a directory, returning a dict of paths, and their hashes.
        Behaves the same as check_file, in terms of interaction with
        the cache, on the individual file level.

        """
        if not os.path.lexists(reference):
            raise ContentVerificationMissingReferenceError
        elif not os.path.isdir(reference):
            raise ContentVerificationReferenceTypeError
        out = {}
        for directory, subdirectory, files in os.walk(reference):
            for file in files:
                reference = f"{directory}/{file}"
                content_hash = self.check_file(reference, force)
                out[reference] = content_hash
        return out

    def start(self, reference: str, consume: int = 0) -> None:
        """
        Initialise a hash object, optionally consuming some
        part of a given file. Buffer the initialised object
        on the instance.

        """
        reference = self._check_path(reference)
        if consume:
            self.buffer[reference] = self._consume_reference(
                reference, num_bytes=consume,
            )
        else:
            self.buffer[reference] = blake3.blake3()

    def update(self, reference: str, content: bytes) -> None:
        """
        Add content to a buffered hash object, associated with
        a given file reference.

        """
        reference = self._check_path(reference)
        b3 = self.buffer.get(reference)
        if not b3:
            raise ContentVerificationMissingHashError
        else:
            b3.update(content)
            return True

    def finish(self, reference: str) -> str:
        """
        Compute, cache, and return the hexdigest
        of a buffered hash object.

        """
        reference = self._check_path(reference)
        b3 = self.buffer.get(reference)
        if not b3:
            raise ContentVerificationMissingHashError
        else:
            content_hash = b3.hexdigest()
            self.backend.store(reference, content_hash)
            return content_hash
