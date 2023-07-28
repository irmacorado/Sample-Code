{{ config(materialized="table", sort=["census_block_geoid"], diststyle='all') }}

with cd as (
    select *
    from {{ ref("stg_census_blocks_to_cd") }}
    where congressionaldistrict != 'ZZ' --Removing unassigned CDs
)

, fips as (
    select *
    from {{ ref("stg_fips_state_county") }}
)

select cd.census_block_geoid

    --geos
    , fips.statecode 
    , fips.countyname 

    , fips.fips_state
    , fips.fips_county
    , cd.census_tract
    , cd.census_bg
    , cd.census_block

    --district
    , cd.congressionaldistrict
from cd 
    left join fips on (cd.fips_state = fips.fips_state and cd.fips_county = fips.fips_county)
