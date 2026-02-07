select "state", "state_name"
from {{ source('reference', 'states_raw') }}