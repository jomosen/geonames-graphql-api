from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from geonames.presentation.api.rest import routes


def create_app() -> FastAPI:
    """Application factory."""
    app = FastAPI(
        title="GeoNames API",
        version="1.0",
        debug=True,
    )

    # REST routes
    app.include_router(routes.router)

    # Root endpoint
    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse(url="/docs")

    return app


app = create_app()
