from typing import List, Optional, Dict, Set, Type, cast
from sqlalchemy.orm import Session, Query
from geonames.application.ports.query_repository_port import QueryRepositoryPort
from geonames.infrastructure.persistence.models.admin_division_model import AdminDivisionModel
from geonames.infrastructure.persistence.models.geoname_model import GeonameModel
from geonames.infrastructure.persistence.models.country_model import CountryModel
from geonames.application.services.geoname_query_expansion_helper import GeonameQueryExpansionHelper


class OrmGeonameQueryRepository(QueryRepositoryPort):

    def __init__(self, 
                 session: Session, 
                 model_class: Type[GeonameModel] = GeonameModel):
        
        self.session = session
        self.model_class = model_class

    def find_all(self, filters: Optional[Dict] = None) -> List[GeonameModel]:
        filters = filters or {}

        query = self.session.query(*self._get_basic_fields())
                
        query = self._apply_expansions(filters, query)

        query = self._apply_basic_filters(filters, query)

        query = self._apply_pagination(filters, query)

        models = cast(Query, query).all()
        return models

    def _get_basic_fields(self):
        return (
            self.model_class.geoname_id, 
            self.model_class.name,
            self.model_class.latitude,
            self.model_class.longitude,
            self.model_class.feature_class,
            self.model_class.feature_code,
            self.model_class.country_code,
            self.model_class.admin1_code,
            self.model_class.admin2_code,
            self.model_class.admin3_code,
            self.model_class.admin4_code,
            self.model_class.population,
            self.model_class.timezone
        )
    
    def _apply_basic_filters(self, filters: Dict, query: Query) -> Query:

        if filters.get("country_code"):
            query = query.filter(self.model_class.country_code == filters["country_code"])

        if filters.get("admin1_code"):
            query = query.filter(self.model_class.admin1_code == filters["admin1_code"])

        if filters.get("admin2_code"):
            query = query.filter(self.model_class.admin2_code == filters["admin2_code"])

        if filters.get("admin3_code"):
            query = query.filter(self.model_class.admin3_code == filters["admin3_code"])

        if filters.get("admin4_code"):
            query = query.filter(self.model_class.admin4_code == filters["admin4_code"])

        if filters.get("min_population"):
            query = query.filter(self.model_class.population >= filters["min_population"])

        if filters.get("max_population"):
            query = query.filter(self.model_class.population <= filters["max_population"])

        if filters.get("feature_class"):
            query = query.filter(self.model_class.feature_class == filters["feature_class"])

        if filters.get("feature_code"):
            query = query.filter(self.model_class.feature_code == filters["feature_code"])

        return query

    def _apply_pagination(self, filters: Dict, query: Query) -> Query:

        if filters.get("limit"):
            query = query.limit(filters["limit"])

        if filters.get("offset"):
            query = query.offset(filters["offset"])

        return query
    
    def _apply_expansions(self, filters: Dict, query: Query) -> Query:

        expand = filters.get("expand")
        if not expand:
            return query

        expand_fields = expand if isinstance(expand, list) else [expand]

        joins = GeonameQueryExpansionHelper.get_required_joins(expand_fields)
        fields = GeonameQueryExpansionHelper.get_required_fields(expand_fields)

        if "country" in joins:
            query = self._expand_country(query, fields)

        if self.model_class.__name__ != "AdminDivisionModel" and "admin1" in joins:
            query = self._expand_admin1(query, fields)

        return query


    def _expand_country(self, query: Query, fields: Set[str]) -> Query:

        query = query.outerjoin(
            CountryModel,
            CountryModel.iso_alpha2 == self.model_class.country_code
        )

        if "country_name" in fields:
            query = query.add_columns(CountryModel.country_name.label("country_name"))

        if "postal_code_regex" in fields:
            query = query.add_columns(CountryModel.postal_code_regex.label("postal_code_regex"))

        return query
    
    def _expand_admin1(self, query: Query, fields: Set[str]) -> Query:

        query = query.outerjoin(
            AdminDivisionModel,
                (AdminDivisionModel.country_code == CountryModel.iso_alpha2)
                & (AdminDivisionModel.admin1_code == self.model_class.admin1_code)
                & (AdminDivisionModel.feature_code == "ADM1")
        )

        if "admin1_name" in fields:
            query = query.add_columns(AdminDivisionModel.name.label("admin1_name"))

        return query