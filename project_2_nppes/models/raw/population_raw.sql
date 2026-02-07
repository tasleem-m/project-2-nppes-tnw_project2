select "POP", "NAME", "state", "county"
from {{ source('raw', 'population_raw') }}