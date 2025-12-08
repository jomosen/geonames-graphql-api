from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from geonames.application.ports.query_repository_port import QueryRepositoryPort
from geonames.domain.entities.country import Country
from geonames.infrastructure.persistence.models.country_model import CountryModel
from geonames.infrastructure.persistence.mappers.country_persistence_mapper import CountryPersistenceMapper


class OrmCountryQueryRepository(QueryRepositoryPort):

    def __init__(self, session: Session):
        self.session = session

    def find_all(self, filters: Optional[Dict] = None) -> List[Country]:
        filters = filters or {}

        query = self.session.query(CountryModel)

        if "iso_alpha2" in filters and filters["iso_alpha2"]:
            query = query.filter(CountryModel.iso_alpha2 == filters["iso_alpha2"])

        if "continent" in filters and filters["continent"]:
            query = query.filter(CountryModel.continent == filters["continent"])

        if "min_population" in filters and filters["min_population"]:
            query = query.filter(CountryModel.population >= filters["min_population"])

        if "max_population" in filters and filters["max_population"]:
            query = query.filter(CountryModel.population <= filters["max_population"])

        if "currency_code" in filters and filters["currency_code"]:
            query = query.filter(CountryModel.currency_code == filters["currency_code"])

        models = query.all()
        return models