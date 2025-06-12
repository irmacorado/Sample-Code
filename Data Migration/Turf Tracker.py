from parsons import Table, Redshift, VAN, GoogleSheets
import os
import datetime
import logging
import json

# Setup enviroment/keys
rs = Redshift(username=username, password=pw, host=host,
              db=database, port=port)

credential_filename = 'credentials.json'
credentials = json.load(open(credential_filename))
sheets = GoogleSheets(google_keyfile_dict=credentials)

natl_sheet='1vfEPbASNf_w8bJCixSj26VGVcDSUFvFBcc8oN64ASqs'

# First query for universe and attempts (aka data tab)
saturation_query = f'''
with universe as (
select uni.statecode
, uni.region
, uni.uniqueprecinctcode
, d.precinctname
, count(distinct dwid) as ppl_in_univ
, count(distinct case when bin_race_bipoc = 'BIPOC' then dwid end) as bipoc_univ
, count(distinct case when bin_race = 'AAPI' then dwid end) as aapi_univ
, count(distinct case when bin_race = 'Black' then dwid end) as black_univ
, count(distinct case when bin_race = 'Latinx' then dwid end) as latinx_univ
, count(distinct case when bin_race = 'Unknown / Other' then dwid end) as other_univ
, count(distinct case when bin_race = 'White' then dwid end) as white_univ
from universe uni
left join icorado.districts_2022 d on (uni.uniqueprecinctcode = d.uniqueprecinctcode and uni.statecode = d.statecode)
where uni.is_canvassable = 1 
group by 1,2,3,4
)
, attempts as (
select statecode
, region 
, uniqueprecinctcode
, count(distinct dwid) as ppl_attempted
, count(distinct case when bin_race_bipoc = 'BIPOC' then dwid end) as bipoc_attempts
, count(distinct case when bin_race = 'AAPI' then dwid end) as aapi_attempts
, count(distinct case when bin_race = 'Black' then dwid end) as black_attempts
, count(distinct case when bin_race = 'Latinx' then dwid end) as latinx_attempts
, count(distinct case when bin_race ='Unknown / Other' then dwid end) as other_attempts
, count(distinct case when bin_race = 'White' then dwid end) as white_attempts
from attempts att
where is_canvassable = 1 
and datecanvassed >= '2022-01-01'
and contacttypeid in (2,16,35,36) --doors
group by 1,2,3
)
, knocks as (
select uni.statecode
, d.uniqueprecinctcode
, count(distinct uni.van_door_hhid) uni_doors
, count(distinct case when att.contacttypeid in (2,16,35,36) then date_trunc('h', datecanvassed)||van_door_hhid end) uni_knocks
, count(distinct case when att.contacttypeid in (2,16,35,36) then van_door_hhid end) uni_doors_ever_knocked
from universe uni
inner join districts d
left join (
    select * from attempts
    where datecanvassed between '2022-01-01' and current_date
    and contacttypeid in (2,16,35,36) --doors
    ) att on (uni.statecode = att.statecode and uni.dwid = att.dwid)
where uni.is_canvassable = 1
)
select universe.statecode as state
, nvl(universe.region,'N/A')  as region
, universe.uniqueprecinctcode
, universe.precinctname
, universe.ppl_in_univ 
, attempts.ppl_attempted
, bipoc_univ
, bipoc_attempts
, aapi_univ
, aapi_attempts 
, black_univ 
, black_attempts 
, latinx_univ 
, latinx_attempts 
, other_univ 
, other_attempts 
, white_univ 
, white_attempts
, uni_doors
, uni_knocks
, uni_doors_ever_knocked
from universe
left join attempts on (universe.statecode = attempts.statecode and universe.uniqueprecinctcode = attempts.uniqueprecinctcode)
left join knocks on (universe.statecode = knocks.statecode and universe.uniqueprecinctcode = knocks.uniqueprecinctcode)
order by 1 asc, 2 asc, 3 asc
'''
saturation_table = rs.query(saturation_query)
print(f"{saturation_table.num_rows} rows found.")
print(saturation_table[:5])

# Push data to spreadsheet
sheets.overwrite_sheet(natl_sheet,
                       worksheet='data',
                       table=saturation_table,
                       user_entered_value=False)

# Second query is for the Passes summary tab
passes_query = f'''
with base as (
select statecode
, region 
, uniqueprecinctcode
, dwid
, count(distinct avccid) as attempts_ytd
from attempts att
where is_canvassable = 1 
and datecanvassed >= '2022-01-01'
and uniqueprecinctcode is not null
and contacttypeid in (2,16,35,36) --doors
group by 1,2,3,4
)
select base.statecode
, nvl(base.region,'N/A') as region 
, base.uniqueprecinctcode
, count(distinct dwid) as one_passes --1 pass
, count(distinct case when attempts_ytd > 1 then dwid end) as two_passes --2 pass
, count(distinct case when attempts_ytd > 2 then dwid end) as three_passes --3 pass
, count(distinct case when attempts_ytd > 3 then dwid end) as four_passes --4 pass
from base
group by 1,2,3
order by 1 asc, 2 asc, 3 asc
'''
passes_table = rs.query(passes_query)
print(f"{passes_table.num_rows} rows found.")
print(passes_table[:5])

# Push the passes-data to spreadsheet
sheets.overwrite_sheet(natl_sheet,
                       worksheet='passes-data',
                       table=passes_table,
                       user_entered_value=False)

# Third query for Canvassable Universes by State tab
universe_query = f'''
select distinct uni.statecode as State
, plan.uni_type as Universe
from universe uni
left join universe_plan plan on (uni.dwid = plan.dwid and uni.statecode=plan.statecode)
where uni.is_canvassable = 1 
order by 1 asc, 2 asc
'''

universe_table = rs.query(universe_query)
print(f"{universe_table.num_rows} rows found.")
print(universe_table[:5])

# Push the universe_table to spreadsheet
sheets.overwrite_sheet(natl_sheet,
                       worksheet='Canvassable Universes by State',
                       table=universe_table,
                       user_entered_value=False)
