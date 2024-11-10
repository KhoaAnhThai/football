{{ config(materialized='table') }}
with Score as(
select home team, 
	case 
		when home_score -away_score = 0 then 1 
		when home_score - away_score <0 then 0
		else 3 end score,
	home_score-away_score goal_difference
from result
union all
select away team, 
	case 
		when home_score - away_score = 0 then 1 
		when home_score - away_score > 0 then 0
		else 3 end score
	,-home_score+ away_score goal_difference
from result
)
select team,sum(case when score = 3 then 1 else 0 end) as won,
		sum(case when score = 1 then 1 else 0 end) as drawn, 
		sum(case when score = 0 then 1 else 0 end) as lost,
		count(*) as total_match ,
		sum(goal_difference) as gd,
		sum(score) points
from Score
group by team
order by points desc,gd desc
