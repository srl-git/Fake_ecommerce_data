import random
from datetime import datetime, timezone

import yaml

from data_generator import Ecommerce
from shared.db_connection import close_db, init_db
from shared.logger import get_logger, setup_logging

setup_logging()
log = get_logger(__name__)


def main() -> None:
    try:
        init_db()

        with open("data_generator/config.yaml") as f:
            config = yaml.safe_load(f.read())

        ecommerce = Ecommerce(locales=config.get("locales"))
        wednesday = datetime.now(tz=timezone.utc).isoweekday() == 3

        if wednesday:
            ecommerce.create_products(
                num_items=random.randint(1, 6),
                **config.get("create_products"),
            )

        ecommerce.create_orders(
            num_orders=random.randint(3, 300),
            max_num_items=7,
        )

        ecommerce.to_csv(
            start_date=datetime.strftime(datetime.now(tz=timezone.utc), "%Y-%m-%d"),
            end_date=datetime.strftime(datetime.now(tz=timezone.utc), "%Y-%m-%d"),
            **config.get("to_csv"),
        )
        log.info(ecommerce)

    except Exception:
        log.exception("Error in data generator")

    finally:
        close_db()


if __name__ == "__main__":
    main()
