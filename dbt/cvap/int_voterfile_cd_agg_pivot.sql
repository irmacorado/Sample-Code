{{ config(materialized="table", sort=["statecode",'congressionaldistrict'], diststyle='even') }}

with base as (
    select * 
    from {{ ref("int_voterfile_cd_agg_raceprob") }} 
)

, pivoted as (
    select 
    --geos
    statecode 
    , congressionaldistrict

    --metrics
    , sum(regvoters_tot) as regvoters_tot
    , sum(regvoters_tot_active) as regvoters_tot_active
    , sum(regvoters_tot_inactive) as regvoters_tot_inactive

    /*sum pivots*/
    
    --regvoters_tot
    , {% for demo in ['bin_race','bin_race_bipoc','bin_gender','bin_age','bin_partyreg','bin_partisanshippropensity','bin_votepropensity','e2020g','e2022g'] %}              
        {{dbt_utils.pivot(
            demo,
            dbt_utils.get_column_values(
                ref("int_voterfile_cd_agg_raceprob")
                    , demo
                    , order_by=demo
                    ),
        agg="sum",
        then_value='regvoters_tot',
        quote_identifiers=False,
        prefix='regvoters_tot_'~demo~'_'
        )}}{% if not loop.last %},{% endif %}
    {% endfor %}
    
    --regvoters_tot_active
    , {% for demo in ['bin_race','bin_race_bipoc','bin_gender','bin_age','bin_partyreg','bin_partisanshippropensity','bin_votepropensity','e2020g','e2022g'] %}              
        {{dbt_utils.pivot(
            demo,
            dbt_utils.get_column_values(
                ref("int_voterfile_cd_agg_raceprob")
                    , demo
                    , order_by=demo
                    ),
        agg="sum",
        then_value='regvoters_tot_active',
        quote_identifiers=False,
        prefix='regvoters_tot_active_'~demo~'_'
        )}}{% if not loop.last %},{% endif %}
    {% endfor %}

    --regvoters_tot_inactive
    , {% for demo in ['bin_race','bin_race_bipoc','bin_gender','bin_age','bin_partyreg','bin_partisanshippropensity','bin_votepropensity','e2020g','e2022g'] %}              
        {{dbt_utils.pivot(
            demo,
            dbt_utils.get_column_values(
                ref("int_voterfile_cd_agg_raceprob")
                    , demo
                    , order_by=demo
                    ),
        agg="sum",
        then_value='regvoters_tot_inactive',
        quote_identifiers=False,
        prefix='regvoters_tot_inactive_'~demo~'_'
        )}}{% if not loop.last %},{% endif %}
    {% endfor %}
    from base
    {{ dbt_utils.group_by(2) }}
)

select * 
from pivoted
