# src/models.py
from dataclasses import dataclass

@dataclass
class Bs4ElementPath:
    jobs_path: str
    title_path: str
    link_path: str
    location_path: str
    description_path: str

@dataclass
class Bs4Element:
    name: str
    url: str
    pages_to_crawl: int
    start_point: int
    strategy: str
    follow_link: str
    inner_link_tag: str
    elements_path: Bs4ElementPath
