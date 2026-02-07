import utils.s3_helper as s3
import utils.api_helper as api
import my_logger
import utils.duckdb_helper as d

def main():
    logger = my_logger.get_logger()
    
## NPPES
    nppes_raw = s3.get_select_raw_data_from_AWS("de-project2-nppes", "team-room-6/raw/nppes/nppes_raw.csv", ["NPI", "Provider Organization Name (Legal Business Name)", 
                  "Provider Last Name (Legal Name)", "Provider First Name", "Provider Middle Name", 
                  "Provider Business Practice Location Address City Name", "Provider Business Practice Location Address State Name", 
                  "Provider Business Practice Location Address Postal Code", "Healthcare Provider Taxonomy Code_1"], logger)

    print(nppes_raw.explain(optimized=True))

    lazy_nppes_raw = nppes_raw.collect(engine="streaming")

    d.convert_to_duckdb("raw", "nppes_raw", lazy_nppes_raw, logger)

## Specialties
    specialty_raw = s3.get_all_raw_data_from_AWS("de-project2-nppes", "team-room-6/raw/reference/nucc_taxonomy_250.csv", logger)

    print(specialty_raw.explain(optimized=True))

    lazy_specialty_raw = specialty_raw.collect(engine="streaming")

    d.convert_to_duckdb("raw", "provider_specialty_raw", lazy_specialty_raw, logger)

## States
    state_raw = s3.get_select_raw_data_from_AWS("de-project2-nppes", "team-room-6/raw/reference/ssa_fips_state_county_2025.csv", ["state", "state_name"], logger)

    print(state_raw.explain(optimized=True))

    lazy_state_raw = state_raw.collect(engine="streaming")

    d.convert_to_duckdb("reference", "states_raw", lazy_state_raw, logger)

## Population
    population_raw = api.get_data_from_API(logger)

    s3.upload_raw_data_to_AWS(population_raw, "de-project2-nppes", "team-room-6/raw/geographic/population_api_data.csv", logger)

    d.convert_to_duckdb("raw", "population_raw", population_raw, logger)

## Zip_county
    zip_county_raw = s3.get_raw_excel_data_from_AWS("de-project2-nppes", "team-room-6/raw/reference/ZIP_COUNTY_032025.xlsx", logger)

    d.convert_to_duckdb("reference", "zip_county_raw", zip_county_raw, logger)

if __name__ == "__main__":
    main()
