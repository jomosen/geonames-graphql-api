from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session

from shared.infrastructure.persistence.database.base_sql_connector import BaseSqlConnector


class PostgreSQLConnector(BaseSqlConnector):
    def __init__(self, db_url: str, echo: bool = False):
        self._engine: Engine = create_engine(
            db_url,
            pool_pre_ping=True,      # checks connection health
            pool_size=10,            # optional: defaults recommended for Postgres
            max_overflow=20,         # optional
            pool_recycle=1800,       # avoid stale connections
            future=True,
            echo=echo,
        )
        self._session_factory = scoped_session(
            sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self._engine,
            )
        )

    @property
    def engine(self) -> Engine:
        return self._engine

    def get_session(self) -> Session:
        return self._session_factory()

    def dispose(self) -> None:
        self._engine.dispose()
