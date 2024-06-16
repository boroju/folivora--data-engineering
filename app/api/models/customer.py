from typing import Optional
from pydantic import BaseModel


class DefaultAddress(BaseModel):
    street: Optional[str]
    number: Optional[str]
    floor: Optional[str]
    city: Optional[str]
    country: Optional[str]
    created_at: Optional[str]
    id: Optional[str]
    locality: Optional[str]
    province: Optional[str]
    updated_at: Optional[str]
    zipcode: Optional[str]


class Customer(BaseModel):
    id: int
    name: str
    email: str
    identification: str
    phone: str
    total_spent: float
    total_spent_currency: str
    active: bool
    last_order_id: Optional[int]
    first_interaction: str
    created_at: str
    updated_at: str
    default_address: Optional[DefaultAddress]
    billing_country: Optional[str]
    accepts_marketing: bool
    accepts_marketing_updated_at: str
    processed_date: str
