from typing import List, Optional, Dict, Type, TypeVar
from sqlalchemy import text
from sqlalchemy.orm import Session
from geonames.domain.repositories.geoname_repository import GeonameRepository
from geonames.domain.entities.geoname import Geoname
from geonames.infrastructure.persistence.models.admin_division_model import AdminDivisionModel
from geonames.infrastructure.persistence.models.alternate_name_model import AlternateNameModel
from geonames.infrastructure.persistence.models.country_model import CountryModel
from geonames.infrastructure.persistence.models.geoname_model import GeonameModel
from geonames.infrastructure.persistence.mappers.geoname_persistence_mapper import GeonamePersistenceMapper


class OrmGeonameRepository(GeonameRepository):

    def __init__(self, 
                 session: Session, 
                 model_class: Type[GeonameModel] = GeonameModel):
        
        self.session = session
        self.model_class = model_class

    def find_by_id(self, geoname_id: int) -> Optional[Geoname]:
        record = self.session.get(self.model_class, geoname_id)
        return GeonamePersistenceMapper.to_entity(record) if record else None

    def find_all(self, filters: Optional[Dict] = None) -> List[Geoname]:
        filters = filters or {}

        query = self.session.query(self.model_class)

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
        if filters.get("name_like"):
            query = query.filter(self.model_class.name.ilike(f"%{filters['name_like']}%"))

        if filters.get("limit"):
            query = query.limit(filters["limit"])
        if filters.get("offset"):
            query = query.offset(filters["offset"])

        models = query.all()
        return [GeonamePersistenceMapper.to_entity(m) for m in models]

    def find_all_enriched(self, filters: Optional[Dict] = None) -> List[Geoname]:
        filters = filters or {}

        iso_language = filters.get("iso_language", "en")
        country_code = filters.get("country_code")

        # Base SELECT with extra fields
        query = (
            self.session.query(
                self.model_class,
                AlternateNameModel.alternate_name.label("admin1_name"),
                CountryModel.country_name.label("country_name"),
                CountryModel.postal_code_regex.label("postal_code_regex"),
            )
            # JOIN country
            .outerjoin(
                CountryModel,
                CountryModel.iso_alpha2 == self.model_class.country_code
            )
            # JOIN admin_geonames (ADM1 only)
            .outerjoin(
                AdminDivisionModel,
                (AdminDivisionModel.country_code == CountryModel.iso_alpha2)
                & (AdminDivisionModel.admin1_code == self.model_class.admin1_code)
                & (AdminDivisionModel.feature_code == "ADM1")
            )
            # JOIN alternate_names (language + flags inside the join)
            .outerjoin(
                AlternateNameModel,
                (AlternateNameModel.geoname_id == AdminDivisionModel.geoname_id)
                & (AlternateNameModel.iso_language == iso_language)
                & (AlternateNameModel.is_preferred_name == 0)
                & (AlternateNameModel.is_short_name == 1)
            )
        )

        # FILTERS (always GeoNameModel)
        if country_code:
            query = query.filter(self.model_class.country_code == country_code)
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
        if filters.get("name_like"):
            query = query.filter(self.model_class.name.ilike(f"%{filters['name_like']}%"))

        rows = query.all()
        entities: List[Geoname] = []

        for model, admin1_name, country_name, postal_code_regex in rows:
            entity = GeonamePersistenceMapper.to_entity(
                model,
                admin1_name=admin1_name,
                country_name=country_name,
                postal_code_regex=postal_code_regex,
            )
            entities.append(entity)

        return entities

    
    def save(self, entity: Geoname) -> None:
        model: GeonameModel = GeonamePersistenceMapper.to_model(entity, model_class=self.model_class)
        existing = self.session.get(self.model_class, model.geoname_id)
        if existing:
            for attr, value in vars(model).items():
                if hasattr(existing, attr):
                    setattr(existing, attr, value)
        else:
            self.session.add(model)

        self.session.commit()

    def count_all(self) -> int:
        count = self.session.query(self.model_class).count()
        return count
    
    def bulk_insert(self, entities: List[Geoname]) -> None:
        models = [GeonamePersistenceMapper.to_model(entity, model_class=self.model_class) for entity in entities]
        self.session.bulk_save_objects(models)
        self.session.commit()

    def truncate(self):
        table_name = self.model_class.__tablename__
        self.session.execute(text(f"TRUNCATE TABLE {table_name}"))
        self.session.commit()
