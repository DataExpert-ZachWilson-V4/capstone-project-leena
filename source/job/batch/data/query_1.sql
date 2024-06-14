Top wicket takers


select
    sum(wickets),
    player_name
from
    deliveries_facts fct
join
     dim_player p
on
    fact.player_id = p.player_id
group by
    player_id