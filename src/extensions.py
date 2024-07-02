from typing import TypeAlias, Callable, Coroutine, Any
from psycopg2.extensions import cursor
import aiohttp

# Forward references
Bs4Config: TypeAlias = 'Bs4Config' # type: ignore
ApiConfig: TypeAlias = 'ApiConfig' # type: ignore
RssConfig: TypeAlias = 'RssConfig' # type: ignore

Bs4ElementPath: TypeAlias = 'Bs4ElementPath' # type: ignore
ApiElementPath: TypeAlias = 'ApiElementPath' # type: ignore

# Type aliases for complex types
CrawlFunc: TypeAlias = Callable[
    [
        Callable[[aiohttp.ClientSession], Coroutine[Any, Any, str]],
        aiohttp.ClientSession,
        Any,  # This could be Bs4Config | ApiConfig | RssConfig, but using Any for simplicity
        cursor,
        bool,
    ],
    Coroutine[Any, Any, dict[str, list[str]]]
]

Bs4Args: TypeAlias = 'Bs4Args' # type: ignore
ApiArgs: TypeAlias = 'ApiArgs' # type: ignore
RssArgs: TypeAlias = 'RssArgs' # type: ignore