# JobsCrawler

-------
## License

[![CC BY-NC-SA 4.0][cc-by-nc-sa-shield]][cc-by-nc-sa]

This work is licensed under a
[Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License][cc-by-nc-sa].

[![CC BY-NC-SA 4.0][cc-by-nc-sa-image]][cc-by-nc-sa]

[cc-by-nc-sa]: http://creativecommons.org/licenses/by-nc-sa/4.0/
[cc-by-nc-sa-image]: https://licensebuttons.net/l/by-nc-sa/4.0/88x31.png
[cc-by-nc-sa-shield]: https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg

## Project Overview

JobsCrawler is designed to aggregate job listings from a variety of sources including job boards, custom RSS feeds, and traditional APIs. It utilizes a combination of `Selenium`, `BeautifulSoup (bs4)`, custom `RSS readers`, and direct API calls with the `requests` library to scrape job postings and save them to a PostgreSQL database, either locally or managed. 

The project operates asynchronously, with each tool having its own unique strategy implemented in separate async files. These strategies are orchestrated together in `main.py`.

The `embeddings` branch is an enhanced version of the project that focuses on embedding the results from each module and offers improved modularity. Notably, this branch is compatible with Windows and can be implemented out of the box for Retrieval-Augmented Generation (RAG), unlike the main branch.

-------

## Table of Contents

- [How It Works](#how-it-works)
- [Forks](#forks)
- [Quickstart - Main Branch](#quickstart-main-branch)
- [Quickstart - Embeddings Branch](#quickstart-embeddings-branch)
-------

## How It Works

Each module within JobsCrawler is configured with two JSON schemas: `prod` and `test`. These schemas define the parameters for the scraping process, such as CSS selectors, the strategy to be used, and the number of pages to crawl. Here is an example JSON object for the site `4dayweek.io`:

```json
{
    "name": "https://4dayweek.io",
    "url": "https://4dayweek.io/remote-jobs/fully-remote/?page=",
    "pages_to_crawl": 1,
    "start_point": 1,
    "strategy": "container",
    "follow_link": "yes",
    "inner_link_tag": ".row.job-content-wrapper .col-sm-8.cols.hero-left",
    "elements_path": [
        {
            "jobs_path": ".row.jobs-list",
            "title_path": ".row.job-tile-title",
            "link_path": ".row.job-tile-title h3 a",
            "location_path": ".job-tile-tags .remote-country",
            "description_path": ".job-tile-tags .tile-salary"
        }
    ]
}
```

- To add a new website to the crawler, create a corresponding JSON object with the required parameters. 
- For testing, set `pipeline=TEST` to ensure the correct data is being scraped, and save the schema to the appropriate JSON file (e.g., `bs4_test.json`). 
- Before running any tests, ensure that your environment variables are correctly set up. 
- For debugging, enable logging as there are numerous log statements placed at common breakpoints.
- Typically, each batch of data is saved in a CSV file for manual inspection, aiding in data cleaning.

-------
## Forks
### Main Branch
The `main` branch contains the core functionality of JobsCrawler. It is not compatible with Windows due to certain dependencies.

### Embeddings Branch
The `embeddings` branch is an updated version that includes embedding capabilities and is modularized for better integration with RAG models. This branch is Windows compatible.

-------
## Quickstart - Main Branch
To get started with the main branch:

1. Ensure you have Python and pip installed.
2. Clone the repository and navigate to the project directory.
3. Install the required dependencies using pip: `pip install -r requirements.txt`
4. Set up your `.env` file based on the `.env.example` provided.
-------

## Quickstart - Embeddings Branch
For the embeddings branch:

1. Ensure you have Conda installed.
2. Clone the repository and switch to the embeddings branch.
3. Set up the Conda environment: `conda env create -f environment.yml`
4. Set up your `.env` file based on the `.env.example` provided.
-------