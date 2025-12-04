from pydantic import BaseModel, Field
from typing import Optional
from datetime import date


class GeoNameDTO(BaseModel):
    """Data Transfer Object for GeoName entities (used in API responses)."""

    geoname_id: int = Field(..., description="Unique GeoNames identifier")
    name: str = Field(..., description="Official geographical name")
    asciiname: Optional[str] = Field(None, description="ASCII-transliterated name")
    alternatenames: Optional[str] = Field(None, description="Comma-separated list of alternate names")
    latitude: float = Field(..., description="Latitude in decimal degrees")
    longitude: float = Field(..., description="Longitude in decimal degrees")
    feature_class: str = Field(..., description="Feature class (e.g., A, H, L, P, R, S, T, U, V)")
    feature_code: str = Field(..., description="Feature code (e.g., ADM1, PPLA, PPLC)")
    country_code: str = Field(..., description="ISO country code (e.g., 'ES')")
    cc2: Optional[str] = Field(None, description="Alternate country codes")
    admin1_code: Optional[str] = Field(None, description="First-order administrative division")
    admin2_code: Optional[str] = Field(None, description="Second-order administrative division")
    admin3_code: Optional[str] = Field(None, description="Third-order administrative division")
    admin4_code: Optional[str] = Field(None, description="Fourth-order administrative division")
    population: Optional[int] = Field(0, description="Population count")
    elevation: Optional[int] = Field(None, description="Elevation in meters")
    dem: Optional[int] = Field(None, description="Digital elevation model (in meters)")
    timezone: Optional[str] = Field(None, description="Time zone name")
    modification_date: Optional[date] = Field(None, description="Last modification date (YYYY-MM-DD)")
    postal_code_regex: Optional[str] = Field(None, description="Postal code regex pattern")
    country_name: Optional[str] = Field(None, description="Full country name")
    admin1_name: Optional[str] = Field(None, description="Name of the first-order administrative division")
