from typing import Type, Dict
from sqlalchemy.orm import Session, Query
from geonames.application.services.geoname_query_expansion_helper import GeonameQueryExpansionHelper
from geonames.infrastructure.persistence.models.admin_division_model import AdminDivisionModel
from .orm_geoname_query_repository import OrmGeonameQueryRepository


class OrmAdminDivisionQueryRepository(OrmGeonameQueryRepository):

    def __init__(self, 
                 session: Session, 
                 model_class: Type[AdminDivisionModel] = AdminDivisionModel):
        
        self.session = session
        self.model_class = model_class

    def _find_all(self, filters: dict):
        return super().find_all(filters)
    
    def _apply_expansions(self, filters: Dict, query: Query) -> Query:

        expand = filters.get("expand")
        if not expand:
            return query

        expand_fields = expand if isinstance(expand, list) else [expand]

        joins = GeonameQueryExpansionHelper.get_required_joins(expand_fields)
        fields = GeonameQueryExpansionHelper.get_required_fields(expand_fields)

        if "country" in joins:
            query = self._expand_country(query, fields)

        return query