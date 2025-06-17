import argparse
import random
from datetime import datetime, timezone

import yaml

from data_generator import Ecommerce
from shared.db_connection import close_db, init_db
from shared.logger import get_logger, setup_logging

setup_logging()
log = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=False, help="ISO date, e.g., 2025-06-17")
    parser.add_argument("--create_products", action="store_true", help="Flag to create products")
    return parser.parse_args()


def main() -> None:
    try:
        init_db()
        args = parse_args()
        run_date = datetime.fromisoformat(args.run_date) if args.run_date else datetime.now(tz=timezone.utc)
        create_products = args.create_products

        with open("data_generator/config.yaml") as f:
            config = yaml.safe_load(f.read())

        ecommerce = Ecommerce(locales=config.get("locales"))
        is_wednesday = datetime.now(tz=timezone.utc).isoweekday() == 3

        if create_products or is_wednesday:
            ecommerce.create_products(
                num_items=random.randint(1, 6),
                creation_date=run_date,
                **config.get("create_products"),
            )

        ecommerce.create_orders(
            num_orders=random.randint(3, 300),
            max_num_items=7,
            date_created=run_date,
        )

        ecommerce.to_csv(
            start_date=datetime.strftime(run_date, "%Y-%m-%d"),
            end_date=datetime.strftime(run_date, "%Y-%m-%d"),
            timestamp=datetime.strftime(run_date, "%Y-%m-%d"),
            messy_data=True,
        )
        log.info(ecommerce)

    except Exception:
        log.exception("Error in data generator")

    finally:
        close_db()


if __name__ == "__main__":
    main()
