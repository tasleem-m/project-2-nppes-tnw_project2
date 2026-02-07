select "Code", "Grouping", "Classification", "Specialization", "Definition", "Notes", "Display Name", "Section"
from {{ source('raw', 'provider_specialty_raw') }}