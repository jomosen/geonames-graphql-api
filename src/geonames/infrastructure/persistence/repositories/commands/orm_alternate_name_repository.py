from typing import List, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session
from geonames.domain.entities.alternate_name import AlternateName
from geonames.domain.repositories.alternate_name_repository import AlternateNameRepository
from geonames.infrastructure.persistence.models.alternate_name_model import AlternateNameModel
from geonames.infrastructure.persistence.mappers.alternatename_persistence_mapper import AlternateNamePersistenceMapper


class OrmAlternateNameRepository(AlternateNameRepository):

    def __init__(self, session: Session):
        self.session = session
    
    def save(self, entity: AlternateName) -> None:
        model = AlternateNamePersistenceMapper.to_model(entity)
        self.session.merge(model)
        self.session.commit()
    
    def bulk_insert(self, entities: List[AlternateName]) -> None:
        models = [
            AlternateNamePersistenceMapper.to_model(entity) for entity in entities
        ]
        self.session.bulk_save_objects(models)
        self.session.commit()

    def truncate(self):
        table_name = AlternateNameModel.__tablename__
        self.session.execute(text(f"TRUNCATE TABLE {table_name}"))
        self.session.commit()
