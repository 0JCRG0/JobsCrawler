from typing import Protocol, Callable, Coroutine, Any, runtime_checkable
from aiohttp import ClientSession
from psycopg2.extensions import cursor


class Bs4ElementPath(Protocol):
    jobs_path: str
    title_path: str
    link_path: str
    location_path: str
    description_path: str


class ApiElementPath(Protocol):
    dict_tag: str
    title_tag: str
    link_tag: str
    description_tag: str
    pubdate_tag: str
    location_tag: str
    location_default: str

@runtime_checkable
class Bs4Config(Protocol):
    name: str
    url: str
    pages_to_crawl: int
    start_point: int
    strategy: str
    follow_link: str
    inner_link_tag: str
    elements_path: Bs4ElementPath

@runtime_checkable
class ApiConfig(Protocol):
    name: str
    url: str
    class_json: int
    follow_link: str
    inner_link_tag: str
    elements_path: ApiElementPath

@runtime_checkable
class RssConfig(Protocol):
    url: str
    title_tag: str
    link_tag: str
    description_tag: str
    location_tag: str
    follow_link: str
    inner_link_tag: str
