from typing import Type
from sqlalchemy.orm import Session
from geonames.infrastructure.persistence.models.city_model import CityModel
from .orm_geoname_query_repository import OrmGeonameQueryRepository


class OrmCityQueryRepository(OrmGeonameQueryRepository):

    def __init__(self, 
                 session: Session, 
                 model_class: Type[CityModel] = CityModel):
        
        self.session = session
        self.model_class = model_class