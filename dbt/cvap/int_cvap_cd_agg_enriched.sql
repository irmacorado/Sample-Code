{{ config(materialized="table", sort=["statecode",'congressionaldistrict'], diststyle='even') }}

with census as (
    select * 
    from {{ ref("int_cvap_cd_agg_pivot") }} 
)

, voterfile as (
    select * 
    from {{ ref("int_voterfile_cd_agg_pivot") }} 
)

, joined as (
    select 
        census.*
        , {{ dbt_utils.star(from=ref('int_voterfile_cd_agg_pivot')
                            , except=['statecode'
                                    ,'congressionaldistrict'
                                    ,'regvoters_tot_bin_race_bipoc_none'
                                    ,'regvoters_tot_active_bin_race_bipoc_none'
                                    ,'regvoters_tot_inactive_bin_race_bipoc_none']
            ) }}
    from census 
    left join voterfile 
        on (census.statecode = voterfile.statecode 
            and census.congressionaldistrict = voterfile.congressionaldistrict)
)

select *
from joined 
