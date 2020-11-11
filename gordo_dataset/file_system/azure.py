from dataclasses import dataclass


@dataclass(frozen=True)
class ADLSecret:
    tenant_id: str
    client_id: str
    client_secret: str
