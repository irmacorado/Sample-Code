{{ config(materialized="table", sort=["census_bg_geoid"], distkey='census_bg_geoid') }}

with cvap as (
    select * 
    from {{ ref("stg_census_cvap_2021_bg") }} 
)
, renamed as (
    select census_bg_geoname
    , census_bg_geoid
    , fips_state
    , fips_county
    , census_tract
    , census_bg 
    , case
        when line_title = 'American Indian or Alaska Native Alone' then 'Native American'
        when line_title = 'Asian Alone' then 'AAPI'
        when line_title = 'Black or African American Alone' then 'Black'
        when line_title = 'Native Hawaiian or Other Pacific Islander Alone' then 'AAPI'
        when line_title = 'White Alone' then 'White'
        when line_title = 'American Indian or Alaska Native and White' then 'Native American'
        when line_title = 'Asian and White' then 'AAPI'
        when line_title = 'Black or African American and White' then 'Black'
        when line_title = 'American Indian or Alaska Native and Black or African American' then 'Unknown / Other'
        when line_title = 'Remainder of Two or More Race Responses' then 'Unknown / Other'
        when line_title = 'Hispanic or Latino' then 'Latinx'
        else line_title end as bin_race
    , case 
        when bin_race in ('White', 'Unknown / Other') then bin_race
        else 'BIPOC' end as bin_race_bipoc
    , sum(cit_est) as cit_est
    , sum(cvap_est) as cvap_est
    
    from cvap    
    where 
        line_number not in (1,2) --remove aggregated lines (1 = Total and 2 = Not Hispanic/Latino)

    {{ dbt_utils.group_by(8) }}
)

select *
from renamed
