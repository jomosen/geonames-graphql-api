from pydantic import Field
from typing import Optional

from geonames.application.dtos.geoname_dto import GeoNameDTO


class GeoNameDTO(GeoNameDTO):
    """Data Transfer Object for GeoName extended entities (used in API responses)."""

    admin1_name: Optional[str] = Field(None, description="Name of the first-order administrative division")
    country_name: Optional[str] = Field(None, description="Full country name")
    postal_code_regex: Optional[str] = Field(None, description="Postal code regex pattern")