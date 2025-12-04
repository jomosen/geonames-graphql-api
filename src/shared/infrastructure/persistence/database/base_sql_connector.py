from abc import ABC, abstractmethod
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


class BaseSqlConnector(ABC):

    @property
    @abstractmethod
    def engine(self) -> Engine:
        ...

    @abstractmethod
    def get_session(self) -> Session:
        ...

    @abstractmethod
    def dispose(self) -> None:
        ...
