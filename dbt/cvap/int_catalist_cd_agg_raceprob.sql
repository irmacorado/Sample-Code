{{ config(materialized='table', sort=['statecode','congressionaldistrict'], dist = 'even') }}

with base as (
    select *
    from {{ ref('int_voterfile_binned') }}
    where 
        deceased is FALSE
        and voterstatus in ('active','inactive')
)

{% for race in ['white','black','latinx','aapi','nativeamerican','other']%}
    , {{race}} as (
        select 
        --geos
        statecode 
        , congressionaldistrict

        --demos
        , {% if race =='nativeamerican' %}'Native American'{% elif race =='aapi' %}'{{race|upper}}'{% elif race =='other' %}'Unknown / Other'{% else %}'{{race|capitalize}}'{% endif %} as bin_race 
        /*we only want BIPOC aggregated since White and Other will already be available with bin_race*/
        , {% if race in ('black','latinx','aapi','nativeamerican') %}'BIPOC'{% else %} null {% endif %} as bin_race_bipoc
        , bin_age 
        , bin_gender 
        , bin_partyreg

        --models
        , partisanshippropensity
        , votepropensity
        , bin_partisanshippropensity
        , bin_votepropensity

        --vote history
        , case when e2020gst = statecode and e2020gvm is not null then 'voted' else 'didnt vote' end as e2020g
        , case when e2022gst = statecode and e2022gvm is not null then 'voted' else 'didnt vote' end as e2022g
        
        --metric
        {% set column = 'prob_race_' + race|string %}
        /* here we use race probabilities instead of counting the individual people to help with the undercounting of BIPOC voters and the overcounting of White voters */
        , sum({{ column }}) as regvoters_tot
        , sum(case when voterstatus = 'active' then {{ column }} end) as regvoters_tot_active
        , sum(case when voterstatus = 'inactive' then {{ column }} end) as regvoters_tot_inactive
        from base
        {{ dbt_utils.group_by(13)}}
    )
{% endfor %}


select * from white
union all 
select * from black
union all
select * from latinx 
union all
select * from aapi 
union all 
select * from nativeamerican 
union all
select * from other
