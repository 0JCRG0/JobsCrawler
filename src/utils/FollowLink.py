import aiohttp
import bs4

from src.utils.logger_helper import get_custom_logger

logger = get_custom_logger(__name__)

async def async_follow_link(
    session: aiohttp.ClientSession,
    followed_link: str,
    description_final: str,
    inner_link_tag: str,
    default: str = "NaN",
) -> str:
    async with session.get(followed_link) as link_res:
        if link_res.status == 200:
            logger.info(f"""CONNECTION ESTABLISHED ON {followed_link}\n""")
            link_text = await link_res.text()
            link_soup = bs4.BeautifulSoup(link_text, "html.parser")
            description_tag = link_soup.select_one(inner_link_tag)
            if description_tag:
                description_final = description_tag.text
                return description_final
            else:
                logger.warning(f"No description tag found by 'async_follow_link()' while following: {followed_link}. Setting the description to default.")
                description_final = default
                return description_final
        elif link_res.status == 403:
            logger.warning(
                f"""CONNECTION PROHIBITED WITH BS4 ON 'async_follow_link()'. FOLLOWING: {followed_link}. STATUS CODE: "{link_res.status}". SETTING DESCRIPTION TO DEFAULT."""
            )
            description_final = default
            return description_final
        else:
            logger.warning(
                f"""UNEXPECTED STATUS CODE WITH BS4 ON 'async_follow_link()'. FOLLOWING: {followed_link}. STATUS CODE: "{link_res.status}". SETTING DESCRIPTION TO DEFAULT."""
            )
            description_final = default
            return description_final


async def async_follow_link_title_description(
    session: aiohttp.ClientSession,
    followed_link: str,
    description_final: str,
    inner_link_tag: str,
    title_inner_link_tag: str,
    default: str = "NaN",
):
    async with session.get(followed_link) as link_res:
        if link_res.status == 200:
            logger.debug(f"""CONNECTION ESTABLISHED ON {followed_link}\n""")
            link_text = await link_res.text()
            link_soup = bs4.BeautifulSoup(link_text, "html.parser")
            title_tag = link_soup.select_one(title_inner_link_tag)
            description_tag = link_soup.select_one(inner_link_tag)
            title_final = title_tag.text if title_tag else default
            description_final = description_tag.text if description_tag else default
            return title_final, description_final

        elif link_res.status == 403:
            logger.warning(
                f"""CONNECTION PROHIBITED WITH BS4 ON 'async_follow_link_title_description()'. FOLLOWING: {followed_link}. STATUS CODE: "{link_res.status}". SETTING DESCRIPTION TO DEFAULT."""
            )
            description_final = "NaN"
            return description_final
        else:
            logger.warning(
                f"""UNEXPECTED STATUS CODE WITH BS4 ON 'async_follow_link()'. FOLLOWING: {followed_link}. STATUS CODE: "{link_res.status}". SETTING DESCRIPTION TO DEFAULT."""
            )
            description_final = default
            return description_final




async def async_follow_link_echojobs(
    session: aiohttp.ClientSession, url_to_follow: str, selector: str, default: str
):
    async with session.get(url_to_follow) as r:
        try:
            if r.status == 200:
                logger.debug(
                    f"""CONNECTION ESTABLISHED ON {url_to_follow}. USING FollowLinkEchoJobs()\n"""
                )

                request = await r.text()

                soup = bs4.BeautifulSoup(request, "html.parser")

                div_tag = soup.find("div", {"class": selector})

                if div_tag:
                    description_text = div_tag.get_text()
                    return description_text
                else:
                    logger.warning(
                        f"Setting description to default at AsyncFollowLinkEchoJobs().\nFollowing {url_to_follow}\n"
                    )
                    return default
            else:
                logger.warning(
                    f"""CONNECTION FAILED ON {url_to_follow}. STATUS CODE: "{r.status}". Setting description to default."""
                )
                return default
        except Exception as e:
            logger.warning(
                f"Exception at AsyncFollowLinkEchoJobs(). Following {url_to_follow}\n {e}"
            )
