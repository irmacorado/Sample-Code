{{ config(materialized="table", sort=["statecode","congressionaldistrict"], diststyle='even') }}

with cvap as (
    select * 
    from {{ ref("int_cvap_renamed") }} 
)

, cw as (
    select *
    from {{ ref("cw_census_blockgroup_cd") }}
)

, joined as (
    select cvap.census_bg_geoid
    , cw.statecode 
    , cw.countyname
    , cw.fips_county as countyfips
    , cw.congressionaldistrict
    , cvap.bin_race 
    , cvap.bin_race_bipoc 
    --to calculate the cit and cvap estimates for each cd we first divided up the the total from for each census_bg_geoid by the ratio that a cd overlaps in a bg
    , cw.pct_of_bg_in_cd * cvap.cit_est as cit_est 
    , cw.pct_of_bg_in_cd * cvap.cvap_est as cvap_est 
    from cvap 
    left join cw on (cvap.census_bg_geoid = cw.census_bg_geoid)
)

, aggregated as (
    select 
    --geos
    statecode 
    , countyname 
    , countyfips
    , congressionaldistrict

    --demos
    , bin_race
    , bin_race_bipoc
    
    --estimates
    , sum(cit_est) as cit_est 
    , sum(cvap_est) as cvap_est

    from joined
    {{ dbt_utils.group_by(6) }}
)

select *
from aggregated
