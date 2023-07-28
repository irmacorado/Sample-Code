{{ config(materialized="table", sort=["statecode",'congressionaldistrict'], diststyle='even') }}

-- Returns a list of the columns from a catalist table, so you can then iterate in a for loop later on
{% set column_names = dbt_utils.get_filtered_columns_in_relation(
    from=ref('int_catalist_districts_pivot'), 
    except=[
        'statecode','countyname','countyfips', 'congressionaldistrict','statehousedistrict',
        'statesenatedistrict','bin_race','bin_race_bipoc'
    ])
%}

with census as (
    select * 
    from {{ ref("int_cvap_agg_cd") }} 
)

, voterfile as (
    select statecode 
    , countyfips
    , congressionaldistrict
    , bin_race
    , bin_race_bipoc

    -- sum all the metrics
    , {% for column_name in column_names %}
        {% if not loop.first %}, {% endif %}sum({{column_name}}) as {{column_name}}
    {% endfor %}
    from {{ ref("int_voterfile_districts_pivot") }} 
    {{ dbt_utils.group_by(5) }}
    
)

, joined as (
    select 
        census.*
        , {% for column_name in column_names %} 
            {% if not loop.first %}, {% endif %} {{column_name}}
        {% endfor %}
    from census 
    left join catalist 
        on {{ join_on_multiple_fields(
            field_list=['statecode','countyfips','congressionaldistrict','bin_race','bin_race_bipoc']
            , left_table='census'
            , right_table='voterfile'
          ) 
        }}
)

select *
from joined 
