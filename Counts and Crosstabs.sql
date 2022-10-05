select p.regaddrzip as zip 
, d.countyname as county 
, d.precinctname as precinct 

--general 2018 (vote, abs, early, mail, poll, unknown)
, count(distinct case when v.e2018gvm = 'vote' then p.dwid end) as g2018_vote
, count(distinct case when v.e2018gvm = 'absentee' then p.dwid end) as g2018_absentee
, count(distinct case when v.e2018gvm = 'earlyVote' then p.dwid end) as g2018_ev
, count(distinct case when v.e2018gvm = 'mail' then p.dwid end) as g2018_mail
, count(distinct case when v.e2018gvm = 'polling' then p.dwid end) as g2018_polling
, count(distinct case when v.e2018gvm = 'unknown' then p.dwid end) as g2018_unknown

--race/ethinicty (asian, black, white, latino, native, other, unknown)
, count(distinct case when p.race = 'asian' then p.dwid end) as race_asian
, count(distinct case when p.race = 'black' then p.dwid end) as race_black
, count(distinct case when p.race = 'caucasian' then p.dwid end) as race_white
, count(distinct case when p.race = 'hispanic' then p.dwid end) as race_latinx
, count(distinct case when p.race = 'nativeAmerican' then p.dwid end) as race_native
, count(distinct case when p.race = 'other' then p.dwid end) as race_other
, count(distinct case when p.race = 'unknown' then p.dwid end) as race_unknown

--sex (male, female, unknown)
, count(distinct case when p.gender = 'male' then p.dwid end) as gender_male
, count(distinct case when p.gender = 'female' then p.dwid end) as gender_female
, count(distinct case when p.gender = 'GDU' then p.dwid end) as gender_gdu
, count(distinct case when p.gender = 'unspeficied' then p.dwid end) as gender_unspecified

--total
, count(distinct p.dwid) as total

from catalist.ga_person p 
left join catalist.ga_districts d on (p.dwid = d.dwid)
left join catalist.ga_votehistory v on (p.dwid = v.dwid)

where p.voterstatus in ('active', 'inactive')
group by 1,2,3 
order by 1 asc, 2 asc, 3 asc
