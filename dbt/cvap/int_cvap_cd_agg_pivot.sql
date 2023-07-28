{{ config(materialized="table", sort=["statecode",'congressionaldistrict'], diststyle='even') }}

with census as (select * from {{ ref("int_cvap_cd_renamed") }} )

, pivoted as (
    select 
    --geos
    statecode 
    , congressionaldistrict 
    
    --estimates
    , sum(cit_est) as cit_est 
    , sum(cvap_est) as cvap_est

     /*pivots*/
    , {% for demo in ['bin_race','bin_race_bipoc'] %}
        {% for metric in ['cit_est','cvap_est'] %}                
            {{dbt_utils.pivot(
                demo,
                dbt_utils.get_column_values(
                    ref("int_cvap_cd_renamed")
                        , demo
                        , order_by=demo
                        ),
            agg="sum",
            then_value=metric,
            quote_identifiers=False,
            prefix=metric~'_'~demo~'_'
            )}}{% if not loop.last %},{% endif %}
        {% endfor %}{% if not loop.last %},{% endif %}
    {% endfor %}

    from census
    {{ dbt_utils.group_by(2) }}
)

select *
from pivoted
