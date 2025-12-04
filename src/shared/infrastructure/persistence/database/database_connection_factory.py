from shared.infrastructure.persistence.database.base_sql_connector import BaseSqlConnector

from .mysql_connector import MySQLConnector
from .postgresql_connector import PostgreSQLConnector


class DatabaseConnectionFactory:
    """
    Creates a database connector (MySQL or PostgreSQL) depending on the DB URL
    or environment configuration.

    The application layer should never know about connectors—only sessions.
    """

    def __init__(self, db_url: str, echo: bool = False):
        self._db_url = db_url
        self._echo = echo
        self._connector: BaseSqlConnector = self._create_connector()

    def _create_connector(self):
        url = self._db_url.lower()

        if url.startswith("mysql"):
            return MySQLConnector(self._db_url, echo=self._echo)

        if url.startswith("postgresql"):
            return PostgreSQLConnector(self._db_url, echo=self._echo)

        raise ValueError(f"Unsupported database URL: {self._db_url}")

    @property
    def engine(self):
        return self._connector.engine
    
    def get_session(self):
        return self._connector.get_session()

    def dispose(self):
        self._connector.dispose()
