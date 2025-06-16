from datetime import date

from fastapi import FastAPI, HTTPException
from sqlalchemy import func

from shared.db_connection import get_session
from shared.db_models import ProductsModel
from shared.logger import get_logger

log = get_logger(__name__)

app = FastAPI(title="Fake Ecommerce Data")


@app.get("/")
def root() -> dict:
    return {"message": "Welcome to the Fake Ecommerce Data API"}


@app.get("/products")
def get_products(date_updated: date | None = None) -> list[dict] | dict:
    """
    Retrieve products based on the date they were updated.

    - **date_updated**: Optional date query string in the format YYYY-MM-DD.
    """
    try:
        with get_session() as db:
            query_result = db.query(
                ProductsModel.item_sku,
                ProductsModel.item_price,
                ProductsModel.release_date,
                ProductsModel.date_created,
                ProductsModel.date_updated,
                ProductsModel.active,
            )

            if date_updated:
                query_result = query_result.filter(
                    func.date(ProductsModel.date_updated) == date_updated,
                )

            products_rows = query_result.all()
            if not products_rows:
                raise HTTPException(
                    status_code=404,
                    detail=f"No products found matching date_updated={date_updated}.",
                )

            products_as_dict = [row._asdict() for row in products_rows]
            return products_as_dict

    except HTTPException:
        raise

    except Exception:
        log.exception("Database error")
        raise HTTPException(
            status_code=500,
            detail="Internal server error. Please try again later.",
        )
