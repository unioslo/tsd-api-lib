
import os
import pytest
import shutil
import time

from datetime import datetime, timedelta

import blake3
import psycopg2

from tsdapilib.verifier import ContentVerifier
from tsdapilib.backends import GenericBackend, SQLiteBackend, PostgreSQLBackend

class Resources(object):

    def __init__(self, work_dir: str) -> None:
        """
        Creates:

        /tmp/{work_dir}
        /tmp/{work_dir}/f1
        /tmp/{work_dir}/d1
        /tmp/{work_dir}/d1/f2
        /tmp/{work_dir}/d1/f3
        /tmp/{work_dir}/d2
        /tmp/{work_dir}/d2/f4

        """
        base_dir = '/tmp'
        target_dir = f'{base_dir}/{work_dir}'
        try:
            shutil.rmtree(target_dir)
        except FileNotFoundError:
            pass
        self.target_dir = target_dir
        self.references = {}
        d1 = f'{target_dir}/d1'
        d2 = f'{target_dir}/d2'
        os.makedirs(d1)
        os.makedirs(d2)
        self.references['directories'] = {
            'target_dir': target_dir,
            'd1': d1,
            'd2': d2,
        }
        f1 = f'{target_dir}/f1'
        f2 = f'{d1}/f2'
        f3 = f'{d1}/f3'
        f4 = f'{d2}/f4'
        with open(f1, 'wb') as f:
            f.write(b'Thus I have heard')
        with open(f2, 'wb') as f:
            f.write(b'This is unease')
        with open(f3, 'wb') as f:
            f.write(b'This is the cessation of unease')
        with open(f4, 'wb') as f:
            f.write(b'This is the path to the cessation of unease')
        self.references['files'] = {
            'f1': f1,
            'f2': f2,
            'f3': f3,
            'f4': f4,
        }

    def __enter__(self) -> dict:
        return self.references

    def __exit__(self, type, value, traceback) -> None:
        shutil.rmtree(self.target_dir)


class TestContentVerifier(object):

    work_dir = 'test-content-verifier'

    def test_verifier_ephemeral(self) -> None:
        with Resources(self.work_dir) as r:

            # operations on single files
            f1 = r.get('files').get('f1')
            f2 = r.get('files').get('f2')
            f3 = r.get('files').get('f3')

            # simple operations
            verifier = ContentVerifier()
            assert verifier.backend.storage.get(f1) is None
            verfied = verifier.check_file(f1)
            assert verifier.backend.storage.get(f1) is not None
            b3 = blake3.blake3()
            b3.update(open(f1, 'rb').read())
            assert verfied == b3.hexdigest()

            # incremental processing
            verifier.start(f2)
            for chunk in verifier._lazy_read(f2, chunk_size=2):
                verifier.update(f2, chunk)
            verifier.finish(f2)
            incrementally_computed = verifier.backend.storage.get(f2)
            assert incrementally_computed is not None
            b3 = blake3.blake3()
            b3.update(open(f2, 'rb').read())
            assert incrementally_computed.get('content_hash') == b3.hexdigest()

            # operations on directories
            d1 = r.get('directories').get('d1')

            # with only files
            assert len(verifier.check_directory(d1).keys()) == 2

            # with nested directories
            target_dir = r.get('directories').get('target_dir')
            assert len(verifier.check_directory(target_dir)) == 4

            # forcing a cache refresh
            two_days_ago = int((datetime.now() - timedelta(days=2)).timestamp())
            verifier.backend.storage[f1]['stale_after'] = two_days_ago
            assert verifier.backend.storage.get(f1).get('stale_after') == two_days_ago
            verifier.check_directory(target_dir)
            assert verifier.backend.storage.get(f1).get('stale_after') > two_days_ago


    def run_tests_on_verifier_with_backend_and_config(
        self,
        cls: GenericBackend,
        config: dict,
    ) -> None:
        # note: unsafe sql param substitution is used here
        # due to differences in psycopg2 and sqlite3 dialects
        # being tests only, the trade-off is fine
        with Resources(self.work_dir) as r:

            # operations on single files
            f1 = r.get('files').get('f1')
            f2 = r.get('files').get('f2')
            f3 = r.get('files').get('f3')

            verifier = ContentVerifier(backend=cls(config=config))
            backend = verifier.backend
            engine = verifier.backend.engine

            # first clean anyting left over from previous runs
            with backend._session_scope(engine) as session:
                session.execute(
                    f"delete from content_hashes where reference like '%{self.work_dir}%'"
                )
            with backend._session_scope(engine) as session:
                session.execute(
                    f"select * from content_hashes where reference = '{f1}'"
                )
                result = session.fetchall()
            assert not result

            # simple operations
            verfied = verifier.check_file(f1)
            with backend._session_scope(engine) as session:
                session.execute(
                    f"select * from content_hashes where reference = '{f1}'"
                )
                result = session.fetchall()
            assert result
            b3 = blake3.blake3()
            b3.update(open(f1, 'rb').read())
            assert verfied == b3.hexdigest()

            # incremental processing
            verifier.start(f2)
            for chunk in verifier._lazy_read(f2, chunk_size=2):
                verifier.update(f2, chunk)
            verifier.finish(f2)
            with backend._session_scope(engine) as session:
                session.execute(
                    f"select content_hash from content_hashes where reference = '{f2}'"
                )
                result = session.fetchall()
            assert result is not None
            b3 = blake3.blake3()
            b3.update(open(f2, 'rb').read())
            assert result[0][0] == b3.hexdigest()

            # operations on directories
            # with only files
            d1 = r.get('directories').get('d1')
            assert len(verifier.check_directory(d1).keys()) == 2
            # with nested directories
            target_dir = r.get('directories').get('target_dir')
            assert len(verifier.check_directory(target_dir)) == 4

            # forcing a cache refresh
            # reset the expiry
            two_days_ago = int((datetime.now() - timedelta(days=2)).timestamp())
            with backend._session_scope(engine) as session:
                session.execute(
                    f"update content_hashes set stale_after = {two_days_ago} \
                      where reference = '{f1}'"
                )
            with backend._session_scope(engine) as session:
                session.execute(
                    f"select stale_after from content_hashes where reference = '{f1}'"
                )
                result = session.fetchall()
            assert result[0][0] == two_days_ago
            # now refresh it
            verifier.check_directory(target_dir)
            with backend._session_scope(engine) as session:
                session.execute(
                    f"select stale_after from content_hashes where reference = '{f1}'"
                )
                result = session.fetchall()
            assert result[0][0] > two_days_ago

            # finally clean the db
            with backend._session_scope(engine) as session:
                session.execute(
                    f"delete from content_hashes where reference like '%{self.work_dir}%'"
                )

    def test_verifier_sqlite(self) -> None:
        self.run_tests_on_verifier_with_backend_and_config(
            SQLiteBackend,
            {"path": "/tmp", "name": "tacl-verifier.db"},
        )

    def test_verifier_postgres(self) -> None:
        try:
            self.run_tests_on_verifier_with_backend_and_config(
                PostgreSQLBackend,
                {
                    "dbname": "apilib_db",
                    "user": "apilib_user",
                    "pw": "",
                    "host": "localhost",
                },
            )
        except psycopg2.OperationalError:
            print("missing postgres setup - skipping test_verifier_postgres")
            print("install postgres, and do:")
            print("createuser apilib_user")
            print("createdb -O apilib_user apilib_db")
