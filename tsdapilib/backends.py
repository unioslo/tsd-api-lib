
import psycopg2
import psycopg2.extensions
import psycopg2.pool
import sqlite3

from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional, ContextManager, Any


class GenericBackend(object):

    def __init__(self, config: dict = {}, stale_after: int = 24) -> None:
        self._init_backend(config)
        self.purge()
        self._config = config
        self._stale_after = stale_after

    @property
    def config(self) -> dict:
        return self._config

    @property
    def stale_after(self) -> int:
        return self._stale_after

    def _init_backend(self, config: dict) -> None:
        raise NotImplementedError

    def _put(self, reference: str, content_hash: str, stale_after: int) -> bool:
        # simple, atomic, idempotent put
        raise NotImplementedError

    def _get(self, reference: str) -> Optional[dict]:
        # simple, atomic get
        raise NotImplementedError

    def _clean(self) -> bool:
        raise NotImplementedError

    # public methods

    def store(self, reference: str, content_hash: str) -> bool:
        """
        Persist a content reference and its hash digest,
        along with the time after which it should be considered stale.

        """
        stale_after = int((datetime.now() + timedelta(hours=self.stale_after)).timestamp())
        return self._put(reference, content_hash, stale_after)

    def fetch(self, reference: str) -> Optional[dict]:
        """
        Fetch a reference, its hash, and expiry time from the store.
        If there is not item corresponding to the reference, or if
        the reference has become stale, then return None.

        """
        now = datetime.now().timestamp()
        item = self._get(reference)
        if not item:
            return None
        elif now > item.get('stale_after'):
            return None
        else:
            return item

    def purge(self) -> None:
        """
        Remove all stale entries, in bulk.

        """
        return self._clean()


class EphemeralBackend(GenericBackend):

    storage = OrderedDict()

    def _init_backend(self, config: dict) -> None:
        pass # nothing to do here

    def _put(self, reference: str, content_hash: str, stale_after: int) -> bool:
        self.storage[reference] = {
            'reference': reference,
            'content_hash': content_hash,
            'stale_after': stale_after,
        }

    def _get(self, reference: str) -> Optional[dict]:
        return self.storage.get('reference')

    def _clean(self) -> None:
        new = {}
        now = datetime.now().timestamp()
        for k,v in self.storage:
            if now < v.get('stale_after'):
                new[k] = v
        self.storage = new


class GenericDataBaseBackend(GenericBackend):

    table_definition = """
        content_hashes(
            reference text unique not null,
            content_hash text not null,
            stale_after int not null
        )
    """

    def _init_backend(self, config: dict) -> None:
        self.engine = self._db_init(config)
        with self._session_scope(self.engine) as session:
            session.execute(f"create table if not exists {self.table_definition}")

    def _put(self, reference: str, content_hash: str, stale_after: int) -> bool:
        with self._session_scope(self.engine) as session:
            session.execute(
                self._gen_put_sql(),
                {
                    "reference": reference,
                    "content_hash": content_hash,
                    "stale_after": stale_after,
                }
            )
        return True

    def _get(self, reference: str) -> Optional[dict]:
        rows = []
        with self._session_scope(self.engine) as session:
            session.execute(
                self._gen_get_sql(),
                {"reference": reference}
            )
            rows = session.fetchall()
            cols = [desc[0] for desc in session.description]
        if not rows:
            return None
        else:
            item = {}
            for k,v in zip(cols, rows[0]):
                item[k] = v
            return item

    def _clean(self) -> None:
        with self._session_scope(self.engine) as session:
            session.execute(
                self._gen_clean_sql(),
                {"now": datetime.now().timestamp()}
            )

    def _db_init(self, config: dict) -> Any:
        # return a db engine
        raise NotImplementedError

    @contextmanager
    def _session_scope(self, engine: Any) -> ContextManager[Any]:
        # return a transactional scope
        raise NotImplementedError

    def _gen_put_sql(self) -> str:
        raise NotImplementedError

    def _gen_get_sql(self) -> str:
        raise NotImplementedError

    def _gen_clean_sql(self) -> str:
        raise NotImplementedError


class SQLiteBackend(GenericDataBaseBackend):

    def _db_init(
        self,
        config: dict,
    ) -> sqlite3.Connection:
        path = config.get('path')
        dbname = config.get('name')
        dburl = 'sqlite:///' + path + '/' + dbname
        engine = sqlite3.connect(path + '/' + dbname)
        return engine

    @contextmanager
    def _session_scope(
        self,
        engine: sqlite3.Connection,
    ) -> ContextManager[sqlite3.Cursor]:
        session = engine.cursor()
        try:
            yield session
            session.close()
        except Exception as e:
            session.close()
            engine.rollback()
            raise e
        finally:
            session.close()
            engine.commit()

    def _gen_put_sql(self) -> str:
        return """
            insert into content_hashes (reference, content_hash, stale_after)
            values (:reference, :content_hash, :stale_after)
            on conflict (reference) do update
            set content_hash = excluded.content_hash,
            stale_after = excluded.stale_after
            where reference = excluded.reference
        """
        # how to get returning working

    def _gen_get_sql(self) -> str:
        return "select * from content_hashes where reference = :reference"

    def _gen_clean_sql(self) -> str:
        return "delete from content_hashes where :now > stale_after"


class PostgreSQLBackend(GenericDataBaseBackend):

    def _db_init(self, config: dict) -> psycopg2.pool.SimpleConnectionPool:
        min_conn = 2
        max_conn = 5
        dsn = f"dbname={config['dbname']} user={config['user']} password={config['pw']} host={config['host']}"
        pool = psycopg2.pool.SimpleConnectionPool(
            min_conn, max_conn, dsn,
        )
        return pool

    @contextmanager
    def _session_scope(
        self,
        pool: psycopg2.pool.SimpleConnectionPool,
    ) -> ContextManager[psycopg2.extensions.cursor]:
        engine = pool.getconn()
        session = engine.cursor()
        try:
            yield session
            session.close()
        except Exception as e:
            session.close()
            engine.rollback()
            raise e
        finally:
            session.close()
            engine.commit()
            pool.putconn(engine)

    def _gen_put_sql(self) -> str:
        return """
            insert into content_hashes (reference, content_hash, stale_after)
            values (%(reference)s, %(content_hash)s, %(stale_after)s)
            on conflict (reference) do update
            set content_hash = excluded.content_hash,
            stale_after = excluded.stale_after
            where content_hashes.reference = excluded.reference
        """

    def _gen_get_sql(self) -> str:
        return "select * from content_hashes where reference = %(reference)s"

    def _gen_clean_sql(self) -> str:
        return "delete from content_hashes where %(now)s > stale_after"
