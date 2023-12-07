from httpcore import TimeoutException
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import bs4
import traceback
import asyncio
from asyncio import TimeoutError
from concurrent.futures import ThreadPoolExecutor
import logging
#from handy import LoggingMasterCrawler
import os
from traceback import format_exc

async def async_follow_link(session, followed_link, description_final, inner_link_tag, default):

    async with session.get(followed_link) as link_res:
        if link_res.status == 200:
            print(f"""CONNECTION ESTABLISHED ON {followed_link}""", "\n")
            link_text = await link_res.text()
            link_soup = bs4.BeautifulSoup(link_text, 'html.parser')
            description_tag = link_soup.select_one(inner_link_tag)
            if description_tag:
                description_final = description_tag.text
                return description_final
            else:
                description_final = 'NaN'
                return description_final
        elif link_res.status == 403:
            print(f"""CONNECTION PROHIBITED WITH BS4 ON {followed_link}. STATUS CODE: "{link_res.status}". TRYING WITH SELENIUM""", "\n")
            logging.warning(f"""CONNECTION PROHIBITED WITH BS4 ON {followed_link}. STATUS CODE: "{link_res.status}". TRYING WITH SELENIUM""")
            #START SEL
            options = webdriver.FirefoxOptions()
            options.add_argument('-headless')
            # Start the session
            driver = webdriver.Firefox(options=options)
            driver.get(followed_link)
            description_tag = driver.find_element(By.CSS_SELECTOR, inner_link_tag)
            if description_tag:
                description_final = description_tag.get_attribute("innerHTML")
                return description_final
            else:
                description_final = 'NaN'
                return description_final
        else:
            print(f"""CONNECTION FAILED ON {followed_link}. STATUS CODE: "{link_res.status}". Getting the description from default.""", "\n")
            logging.warning(f"""CONNECTION FAILED ON {followed_link}. STATUS CODE: "{link_res.status}". Getting the description from default.""")
            description_final = default
            return description_final

async def async_follow_link_sel(followed_link, inner_link_tag, driver, fetch_sel, default):
    try:
        await fetch_sel(followed_link, driver)  # Replaced driver.get with await fetch_sel
        print(f"""CONNECTION ESTABLISHED ON {followed_link}""", "\n")
        try:
            # Set up a WebDriverWait instance with a timeout of 10 seconds
            wait = WebDriverWait(driver, 10)
            # Wait for the element to be present in the DOM and to be visible
            description_tag = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, inner_link_tag)))
            description_final = description_tag.get_attribute("innerHTML") if description_tag else "NaN"
            return description_final
        except (NoSuchElementException, TimeoutException, Exception) as e:
            error_message = f"{type(e).__name__} **while** following this link: {followed_link}. {traceback.format_exc()}"
            print(error_message)
            logging.error(f"{error_message}\n")
            return default
    except (NoSuchElementException, TimeoutException, Exception) as e:
            error_message = f"{type(e).__name__} **before** following this link: {followed_link}. {traceback.format_exc()}"
            print(error_message)
            logging.error(f"{error_message}\n")
            return default