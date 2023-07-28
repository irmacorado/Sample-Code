
with source as (

    select * from {{ source('census', 'census_blocks_to_cd') }}

),

renamed as (
    select geoid as census_block_geoid
    , left(census_block_geoid,2) as fips_state
    , substring(census_block_geoid,3,3) as fips_county
    , substring(census_block_geoid,6,6) as census_tract
    , substring(census_block_geoid,12,1) as census_bg --blockgroup
    , right(census_block_geoid,3) as census_block
    , cdfp as congressionaldistrict
    from source
)

select *
from renamed
where fips_state in {{ env_var("DBT_STATEFIPS_TO_INCLUDE") }} 
