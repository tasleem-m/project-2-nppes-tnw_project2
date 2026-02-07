import polars as pl
import requests

def get_data_from_API(logger):

    logger.info("Starting census population data fetch...")

    url = 'https://api.census.gov/data/2023/pep/charv?get=POP,NAME&for=county:*&in=state:*'

    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        if isinstance(data, dict) and 'results' in data:
            raw_rows = data["results"]
        else:
            raw_rows = data
        
        columns = raw_rows[0]
        rows = raw_rows[1:]

        pop_data = pl.DataFrame(rows, schema=columns)

        logger.info(f'Loaded {pop_data.height} population from API')
        return pop_data

    except Exception as e:
        logger.error(f'Failed to load population from API: {e}', exc_info=True)
        return None