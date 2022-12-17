
from datetime import date
from os import listdir, mkdir
from os.path import exists, join
import pathlib
from time import sleep

from bs4 import BeautifulSoup
import pandas as pd
import requests


def download_daily(first_year: int=2015, last_year: int=date.today().year):
    """Download all pages from 2015 to the current year

    Args:
        first_year (int, optional): The minimum year to read from. Deafults to 2015.
        last_year (int, optional): The maximum year to read to. Defaults to date.today().year.
    """
    print(f"Requesting all information for year {first_year} to {last_year}")

    if not exists(join(pathlib.Path(__file__).parent.resolve(), 'daily_data')):
        mkdir(join(pathlib.Path(__file__).parent.resolve(), 'daily_data'))

    for year in range(first_year, last_year + 1):
        for day in range(1, 25 + 1):
            try:
                req = requests.get(f'http://www.adventofcode.com/{year}/leaderboard/day/{day}')
                if req.status_code == requests.codes.ok:
                    with open(f'./daily_data/{year}-{str(day).zfill(2)}.html', 'w') as fh:
                        fh.write(str(req.content))
                        print(f"Downloaded year {year}, day {day}")
                else:
                    print(f"Response for request {year}-{day} is {req.status_code}")
            except Exception as e:
                print(f"Failed download/read of {year}-{day} with: {e}")
            sleep(1)


def parse_daily():
    """Parse all of the daily data files.

    Returns:
        List[tuple]: Records list of daily results.
    """

    records = []
    for file in listdir('./daily_data/'):
        # Read the page
        year = file[:4]
        day = file[5:7]
        try:
            page = BeautifulSoup(open(f"./daily_data/{year}-{day}.html"), 'html.parser')
        except:
            print(f"Cannot find {year}-{day}.html")
            continue
        
        # Parse the page
        records.extend([(year, day, 'first' if i > 99 else 'second', entry.find(class_='leaderboard-position').string.strip()[:-1], entry.find(class_='leaderboard-time').string.strip()[-8:]) for i, entry in enumerate(page.main.find_all('div'))])
    
    return records


def make_daily(download: bool=True, first_year: int=2015, last_year: int=date.today().year) -> pd.DataFrame:
    """Construct the daily data parquet file.

    Args:
        download (bool, optional): Whether to download all files again or just parse. Defaults to True.
        first_year (int, optional): The minimum year to read from. Deafults to 2015.
        last_year (int, optional): The maximum year to read to. Defaults to date.today().year.

    Returns:
        pd.DataFrame: Daily dataframe.
    """
    
    # Download all day pages
    if download: download_daily(first_year=first_year, last_year=last_year)
    
    # Parse all day pages
    records = parse_daily()

    # Do some formatting
    records = pd.DataFrame(records, columns=['year', 'day', 'completion', 'position', 'time'])
    records[['year', 'day', 'position']] = records[['year', 'day', 'position']].apply(pd.to_numeric)
    records['time'] = pd.to_timedelta(records['time'])

    # Make parquet file
    records.to_parquet('daily.parquet')

    return records


def get_daily() -> pd.DataFrame:
    """Read and return daily dataframe.

    Returns:
        pd.DataFrame: Dataframe holding daily data.
    """
    return pd.read_parquet('daily.parquet')


def download_yearly(first_year: int=2015, last_year: int=date.today().year):
    """Download all annual summary statistic pages

    Args:
        first_year (int, optional): The minimum year to read from. Deafults to 2015.
        last_year (int, optional): The maximum year to read to. Defaults to date.today().year.
    """

    # Read the 'http://www.adventofcode.com/{year}/stats page
    # main.pre.findall('a')

    if not exists(join(pathlib.Path(__file__).parent.resolve(), 'yearly_data')):
        mkdir(join(pathlib.Path(__file__).parent.resolve(), 'yearly_data'))
    
    print(f"Requesting stats information for year {first_year} to {last_year}")

    for year in range(first_year, last_year + 1):
        try:
            req = requests.get(f'http://www.adventofcode.com/{year}/stats')
            if req.status_code == requests.codes.ok:
                with open(f'./yearly_data/{year}-stats.html', 'w') as fh:
                    fh.write(str(req.content))
                    print(f"Downloaded year {year} stats")
            else:
                print(f"Response for request {year} stats is {req.status_code}")
        except Exception as e:
            print(f"Failed download/read of {year} stats with: {e}")
        sleep(1)


def parse_yearly():
    """Parse all of the annual data files.

    Returns:
        List[tuple]: Records list of annual results.
    """

    records = []
    for file in listdir('./yearly_data/'):
        # Read the page
        year = file[:4]
        try:
            page = BeautifulSoup(open(f"./yearly_data/{year}-stats.html"), 'html.parser')
        except:
            print(f"Cannot find {year}-stats.html")
            continue
        
        # Parse the page
        records.extend([(year, *(entry.getText().split()[:3])) for i, entry in enumerate(page.main.pre.find_all('a'))])
    
    return records


def make_yearly(download: bool=True, first_year: int=2015, last_year: int=date.today().year) -> pd.DataFrame:
    """Construct the yearly data parquet file.

    Args:
        download (bool, optional): Whether to download all files again or just parse. Defaults to True.
        first_year (int, optional): The minimum year to read from. Deafults to 2015.
        last_year (int, optional): The maximum year to read to. Defaults to date.today().year.


    Returns:
        pd.DataFrame: Daily dataframe.
    """
    
    # Download all day pages
    if download: download_yearly(first_year=first_year, last_year=last_year)
    
    # Parse all day pages
    yearly = parse_yearly()

    # Do some formatting
    yearly = pd.DataFrame(yearly, columns=['year', 'day', 'second', 'first'])
    yearly = yearly.apply(pd.to_numeric)
    yearly['first'] = yearly['second'].add(yearly['first'])
    yearly = yearly.melt(['year', 'day'], ['first', 'second'], var_name='completion', value_name='count')
    yearly[['year', 'day', 'count']] = yearly[['year', 'day', 'count']].apply(pd.to_numeric)

    # Make parquet file
    yearly.to_parquet('yearly.parquet')

    return yearly


def get_yearly() -> pd.DataFrame:
    """Read and return yearly dataframe.

    Returns:
        pd.DataFrame: Dataframe holding yearly data.
    """
    return pd.read_parquet('yearly.parquet')
