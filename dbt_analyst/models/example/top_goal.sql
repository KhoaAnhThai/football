{{ config(materialized='table') }}
select last_name||' '|| first_name as player,club ,count(*) as stat 
from goal
group by player_id,last_name,club ,first_name 
order by stat desc
limit 20
