SELECT r.comp_id, r.user_id,r.score, r.u_rank 
            FROM ratings r, competitions c WHERE c.comp_id = 1 AND r.comp_id = c.comp_id 
            AND r.u_rank = (1 - c.rank_offset) ORDER BY r.user_id LIMIT 1;
