from typing import Type
from sqlalchemy.orm import Session
from geonames.infrastructure.persistence.models.admin_division_model import AdminDivisionModel
from .orm_geoname_repository import OrmGeonameRepository


class OrmAdminDivisionRepository(OrmGeonameRepository):

    def __init__(self, 
                 session: Session, 
                 model_class: Type[AdminDivisionModel] = AdminDivisionModel):
        
        self.session = session
        self.model_class = model_class
