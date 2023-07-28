{{ config(materialized="table", sort=["census_bg_geoid"], diststyle='census_bg_geoid') }}

with census_blocks as (
    select * 
    from {{ ref("stg_census_blocks_to_cd") }} 
    where congressionaldistrict != 'ZZ' --Removing unassigned CDs
)

, dedup as (
    select fips_state||fips_county||census_tract||census_bg as census_bg_geoid --drop the last 3 numbers which are census blocks, we just need block 
    , fips_state
    , fips_county
    , census_tract
    , census_bg
    , case when congressionaldistrict = '00' then 1 --At-Large CDs
        when congressionaldistrict = '98' then 1 --non-voting CDs (ie. DC)
        else congressionaldistrict::int --convert to integer
        end as congressionaldistrict
    , count(distinct census_block) as num_census_blocks  
    , row_number() over(partition by census_bg_geoid order by num_census_blocks desc) as cd_most_blocks
    from census_blocks
    {{ dbt_utils.group_by(6) }}
)

select census_bg_geoid
    , congressionaldistrict
from dedup
where cd_most_blocks = 1
