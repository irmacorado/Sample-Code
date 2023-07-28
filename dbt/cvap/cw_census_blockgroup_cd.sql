{{ config(materialized="table", sort=["census_bg_geoid"], diststyle='all') }}

with cd as (
    select *
    , fips_state||fips_county||census_tract||census_bg as census_bg_geoid --blockgroup geoid
    from {{ ref("cw_census_block_cd") }}
)

/*to roll up this crosswalk to the block group level 
we need to select the CD (district) with the most blocks in a block group 
and assign that as the CD for that block group*/

, cd_bg_count as (
    select census_bg_geoid
    , {{ dbt_utils.star(from=ref('cw_census_block_cd'), except=['census_block','census_block_geoid']) }}
    , count(distinct census_block) as num_census_blocks --count the number of census block per blockgroup-district
    , ratio_to_report(num_census_blocks) over(partition by census_bg_geoid) as pct_of_bg_in_cd
    from cd
    {{ dbt_utils.group_by(8) }}
)

select census_bg_geoid

    --geos
    , statecode 
    , countyname 

    , fips_state
    , fips_county
    , census_tract
    , census_bg

    --district
    , congressionaldistrict

    --metric
    , pct_of_bg_in_cd
from cd_bg_count
