
SELECT r.user_id, r.u_rank , r.u_rank - min_rank + 1 rank_in_league, r.score, cb.display_name
FROM ratings r
OUTER JOIN competition_bands AS cb 
WHERE r.comp_id = 1 
AND   cb.comp_id = r.comp_id
AND   r.u_rank BETWEEN cb.min_rank AND cb.max_rank
AND u_rank BETWEEN 1 AND 20
ORDER BY u_rank, user_id LIMIT 350;


