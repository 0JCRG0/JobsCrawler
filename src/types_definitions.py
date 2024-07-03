from typing import Protocol, TypedDict, Callable, Coroutine, Any
from aiohttp import ClientSession
from psycopg2.extensions import cursor


class Bs4ElementPath(TypedDict):
    jobs_path: str
    title_path: str
    link_path: str
    location_path: str
    description_path: str


class Bs4Config(TypedDict):
    name: str
    url: str
    pages_to_crawl: int
    start_point: int
    strategy: str
    follow_link: str
    inner_link_tag: str
    elements_path: Bs4ElementPath


class ApiElementPath(TypedDict):
    dict_tag: str
    title_tag: str
    link_tag: str
    description_tag: str
    pubdate_tag: str
    location_tag: str
    location_default: str


class ApiConfig(TypedDict):
    name: str
    url: str
    class_json: int
    follow_link: str
    inner_link_tag: str
    elements_path: ApiElementPath


class RssConfig(TypedDict):
    url: str
    title_tag: str
    link_tag: str
    description_tag: str
    location_tag: str
    follow_link: str
    inner_link_tag: str


class Bs4ArgsProtocol(Protocol):
    config: type[Bs4Config]
    custom_crawl_func: Callable[
        [
            Callable[[ClientSession], Coroutine[Any, Any, str]],
            ClientSession,
            Bs4Config,
            cursor,
            bool,
        ],
        Coroutine[Any, Any, dict[str, list[str]]],
    ]
    test: bool
    json_prod_path: str
    json_test_path: str
    url_db: str


class ApiArgsProtocol(Protocol):
    config: type[ApiConfig]
    custom_crawl_func: Callable[
        [
            Callable[[ClientSession], Coroutine[Any, Any, str]],
            ClientSession,
            ApiConfig,
            cursor,
            bool,
        ],
        Coroutine[Any, Any, dict[str, list[str]]],
    ]
    test: bool
    json_prod_path: str
    json_test_path: str
    url_db: str


class RssArgsProtocol(Protocol):
    config: type[RssConfig]
    custom_crawl_func: Callable[
        [
            Callable[[ClientSession], Coroutine[Any, Any, str]],
            ClientSession,
            RssConfig,
            cursor,
            bool,
        ],
        Coroutine[Any, Any, dict[str, list[str]]],
    ]
    test: bool
    json_prod_path: str
    json_test_path: str
    url_db: str
