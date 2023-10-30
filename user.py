from dataclasses import dataclass


@dataclass
class User:
    name: str
    favorite_number: int
    favorite_color: str
    address: str
