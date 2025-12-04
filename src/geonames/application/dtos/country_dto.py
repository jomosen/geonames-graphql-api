from pydantic import BaseModel
from typing import Optional


class CountryDTO(BaseModel):
    """Data Transfer Object for Country entities (used in API responses)."""

    geoname_id: int
    iso_alpha2: str
    country_name: str
    iso_alpha3: Optional[str]
    iso_numeric: Optional[int]
    fips_code: Optional[str]
    capital: Optional[str]
    area_sqkm: Optional[float]
    population: Optional[int]
    continent: Optional[str]
    tld: Optional[str]
    currency_code: Optional[str]
    currency_name: Optional[str]
    phone: Optional[str]
    postal_code_format: Optional[str]
    postal_code_regex: Optional[str]
    languages: Optional[str]
    neighbours: Optional[str]
    equivalent_fips_code: Optional[str]
