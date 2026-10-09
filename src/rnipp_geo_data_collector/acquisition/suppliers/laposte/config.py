from pydantic import BaseModel


class LaPosteSupplierConfig(BaseModel):
    endpoint_url: str = (
        "https://datanova.laposte.fr/data-fair/api/v1/datasets/laposte-hexasmal/raw"
    )
    backoff_factor: float = 0.5
    max_retries: int = 5
    connect_timeout: float = 3
    read_timeout: float = 15


class LaPosteEntity(BaseModel):
    name: str
    postal_code: str
    delivery_label: str
    associated_name: str | None


class LaPosteExceptionsToIgnoreOrCorrect(BaseModel):
    root: dict[str, list[LaPosteEntity]] = {}
