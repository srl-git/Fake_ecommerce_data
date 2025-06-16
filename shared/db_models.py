from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import (
    DECIMAL,
    Boolean,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from shared.db_connection import Base


@dataclass
class Product:
    item_sku: str
    item_price: float
    release_date: datetime
    date_created: datetime
    date_updated: datetime
    active: bool
    item_popularity: float


@dataclass
class User:
    user_id: int
    user_name: str
    user_address: str
    user_country: str
    user_email: str
    date_created: datetime


@dataclass
class Order:
    order_line_id: int
    order_id: int
    user_id: int
    item_sku: str
    qty: int
    item_price: int
    date_created: datetime


class ProductsModel(Base):
    __tablename__ = "products"
    __table_args__ = (PrimaryKeyConstraint("item_sku", name="products_pkey"),)
    item_sku: Mapped[str] = mapped_column(Text, primary_key=True)
    item_price: Mapped[float] = mapped_column(Numeric(12, 2))
    release_date: Mapped[datetime] = mapped_column(DateTime)
    date_created: Mapped[datetime] = mapped_column(DateTime)
    date_updated: Mapped[datetime] = mapped_column(DateTime)
    active: Mapped[bool] = mapped_column(Boolean)
    item_popularity: Mapped[float] = mapped_column(Float)

    def to_plain(self) -> Product:
        """
        Convert a ProductsModel instance into a plain Product dataclass object.

        Returns:
            Product: A plain data dataclass object with the same field values as the ProductsModel instance.

        """
        return Product(
            item_sku=self.item_sku,
            item_price=self.item_price,
            release_date=self.release_date,
            date_created=self.date_created,
            date_updated=self.date_updated,
            active=self.active,
            item_popularity=self.item_popularity,
        )


class UsersModel(Base):
    __tablename__ = "users"
    __table_args__ = (PrimaryKeyConstraint("user_id", name="users_pkey"),)
    user_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_name: Mapped[str] = mapped_column(Text)
    user_address: Mapped[str] = mapped_column(Text)
    user_country: Mapped[str] = mapped_column(Text)
    user_email: Mapped[str] = mapped_column(Text)
    date_created: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)

    def to_plain(self) -> User:
        """
        Convert a UsersModel instance into a plain User dataclass object.

        Returns:
            User: A plain data dataclass object with the same field values as the UsersModel instance.

        """
        return User(
            user_id=self.user_id,
            user_name=self.user_name,
            user_address=self.user_address,
            user_country=self.user_country,
            user_email=self.user_email,
            date_created=self.date_created,
        )


class OrdersModel(Base):
    __tablename__ = "orders"
    __table_args__ = (
        ForeignKeyConstraint(
            ["user_id"],
            ["users.user_id"],
            name="orders_user_id_fkey",
        ),
        PrimaryKeyConstraint("order_line_id", name="orders_pkey"),
    )
    order_line_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    order_id: Mapped[int] = mapped_column(Integer)
    user_id: Mapped[int] = mapped_column(Integer)
    item_sku: Mapped[str] = mapped_column(Text)
    qty: Mapped[int] = mapped_column(Integer)
    item_price: Mapped[int] = mapped_column(DECIMAL(12, 2))
    date_created: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(tz=timezone.utc))

    def to_plain(self) -> Order:
        """
        Convert a OrdersModel instance into a plain Order dataclass object.

        Returns:
            Order: A plain data dataclass object with the same field values as the OrdersModel instance.

        """
        return Order(
            order_line_id=self.order_line_id,
            order_id=self.order_id,
            user_id=self.user_id,
            item_sku=self.item_sku,
            qty=self.qty,
            item_price=self.item_price,
            date_created=self.date_created,
        )
