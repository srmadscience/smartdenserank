SELECT r.user_id, r.u_rank + c.rank_offset u_rank, r.u_rank +  cb.min_rank + 1 rank_in_league, r.score, cb.display_name 
FROM competitions c
   , competition_bands cb
   , ratings r 
WHERE r.comp_id = 1 
AND   c.comp_id = cb.comp_id 
AND   cb.comp_id = r.comp_id 
AND   r.u_rank + c.rank_offset BETWEEN cb.min_rank AND cb.max_rank 
AND r.u_rank = -1  ORDER BY r.comp_id, r.u_rank, r.user_id LIMIT 20;
