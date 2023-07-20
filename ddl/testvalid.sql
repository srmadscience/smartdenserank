select foo.comp_id, foo.user_id, foo.score, foo.u_rank, foo.rnk
from 
(select comp_id, user_id, score, u_rank, dense_rank()  
over (order by score desc) as rnk
from ratings) as foo
where foo.rnk != foo.u_rank
and  foo.comp_id = 1
limit 20;
