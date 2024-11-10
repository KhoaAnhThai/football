{{ config(materialized='table') }}
select first_name ||' '|| last_name as player,club as club,count(*) as stat
from cards
where cards.type like '%red%'
group by player_id  ,first_name ,last_name,club 
order by stat desc 