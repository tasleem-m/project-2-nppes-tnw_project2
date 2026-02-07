select "NPI", "Provider Organization Name (Legal Business Name)", 
                  "Provider Last Name (Legal Name)", "Provider First Name", "Provider Middle Name", 
                  "Provider Business Practice Location Address City Name", "Provider Business Practice Location Address State Name", 
                  "Provider Business Practice Location Address Postal Code", "Healthcare Provider Taxonomy Code_1"
from {{ source('raw', 'nppes_raw') }}