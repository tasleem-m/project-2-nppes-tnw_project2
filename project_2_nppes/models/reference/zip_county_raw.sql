select "ZIP", "COUNTY", "USPS_ZIP_PREF_CITY", "USPS_ZIP_PREF_STATE", "RES_RATIO", "BUS_RATIO", "OTH_RATIO", "TOT_RATIO"
from {{ source('reference', 'zip_county_raw') }}