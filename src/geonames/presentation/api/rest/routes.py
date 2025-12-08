from typing import Optional
from fastapi import APIRouter, Depends, Path, Query

from geonames.application.services.city_query_service import CityQueryService
from geonames.application.services.admin_division_query_service import AdminDivisionQueryService
from geonames.application.services.country_query_service import CountryQueryService

from ..dependencies import (
    get_country_query_service,
    get_admin_division_query_service,
    get_city_query_service,
)


router = APIRouter()

@router.get("/countries", tags=["countries"])
def get_countries(
    country_query_service: CountryQueryService = Depends(get_country_query_service),
    iso_alpha2: Optional[str] = Query(None, min_length=2, max_length=2, regex="^(?i)[A-Z]{2}$"),
    continent: Optional[str] = Query(None, min_length=2, max_length=2, regex="^(?i)[A-Z]{2}$"),
    min_population: Optional[int] = Query(None, ge=0),
    max_population: Optional[int] = Query(None, ge=0),
    currency_code: Optional[str] = Query(None, min_length=3, max_length=3, regex="^(?i)[A-Z]{3}$"),
):
    
    filters = {
        "iso_alpha2": iso_alpha2.upper() if iso_alpha2 else None,
        "continent": continent.upper() if continent else None,
        "min_population": min_population,
        "max_population": max_population,
        "currency_code": currency_code.upper() if currency_code else None,
    }

    dtos = country_query_service.list_countries(filters)
    return dtos
    
@router.get("/countries/{country_code}/admin-divisions", tags=["admin-divisions"])
def get_admin_divisions_by_country(
    admin_division_query_service: AdminDivisionQueryService = Depends(get_admin_division_query_service),
    country_code: str = Path(..., min_length=2, max_length=2, regex="^(?i)[A-Z]{2}$"),
    feature_code: Optional[str] = Query(None, min_length=4, max_length=4, regex="^(?i)ADM[1-4]$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    expand: Optional[str] = Query(None),
):

    filters = {
        "country_code": country_code.upper(), 
        "feature_code": feature_code, 
        "limit": limit, 
        "offset": offset,
        "expand": expand.split(",") if expand else None,
    }

    dtos = admin_division_query_service.list_admin_divisions(filters)
    return dtos

@router.get("/countries/{country_code}/cities", tags=["cities"])
def get_cities_by_country(
    city_query_service: CityQueryService = Depends(get_city_query_service),
    country_code: str = Path(..., min_length=2, max_length=2, regex="^(?i)[A-Z]{2}$"),
    admin1_code: Optional[str] = Query(None, min_length=1, max_length=10),
    admin2_code: Optional[str] = Query(None, min_length=1, max_length=10),
    min_population: Optional[int] = Query(None, ge=0),
    language: Optional[str] = Query(None, min_length=2, max_length=7, regex="^(?i)[a-z]{2}(-[A-Z]{2})?$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    expand: Optional[str] = Query(None),
):

    filters = {
        "country_code": country_code.upper(), 
        "admin1_code": admin1_code, 
        "admin2_code": admin2_code, 
        "min_population": min_population, 
        "language": language.lower() if language else None,
        "limit": limit, 
        "offset": offset,
        "expand": expand.split(",") if expand else None,
    }

    dtos = city_query_service.list_cities(filters)
    return dtos
