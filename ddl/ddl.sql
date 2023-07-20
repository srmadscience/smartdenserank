DROP TABLE competition_bands IF EXISTS;
DROP TABLE competitions IF EXISTS;
DROP TABLE ratings IF EXISTS;


CREATE TABLE competitions
	(comp_id BIGINT NOT NULL
    ,max_u_rank BIGINT NOT NULL
     ,rank_offset BIGINT NOT NULL
	, PRIMARY KEY (comp_id)
	);

PARTITION TABLE competitions ON COLUMN comp_id;

create table competition_bands
(comp_id BIGINT NOT NULL
    ,min_rank BIGINT NOT NULL
    ,max_rank BIGINT NOT NULL
    ,display_name varchar(10)
	, PRIMARY KEY (comp_id,min_rank)
	);


CREATE TABLE ratings
	(comp_id BIGINT NOT NULL 
	,user_id BIGINT NOT NULL
	,score BIGINT NOT NULL
    ,u_rank BIGINT NOT NULL
	, PRIMARY KEY (comp_id, user_id)
	);

PARTITION TABLE ratings ON COLUMN comp_id;



create view rows_per_score_view as
SELECT comp_id, score, u_rank, count(*) how_many
FROM ratings 
GROUP BY  comp_id, score, u_rank;

create INDEX rps_ix1 ON rows_per_score_view (comp_id,score,how_many);





CREATE INDEX rix3 ON ratings (comp_id, score);
CREATE INDEX rix4 ON ratings (comp_id, u_rank, user_id);

load classes ../../smartdenserank.jar;


CREATE PROCEDURE 
   PARTITION ON TABLE ratings COLUMN comp_id
   FROM CLASS smartdenserank.ReportRank;

CREATE PROCEDURE 
   PARTITION ON TABLE ratings COLUMN comp_id
   FROM CLASS smartdenserank.QueryRank;

   
CREATE PROCEDURE 
   PARTITION ON TABLE ratings COLUMN comp_id
   FROM CLASS smartdenserank.QueryUserId;

   
   
   
CREATE PROCEDURE 
   PARTITION ON TABLE ratings COLUMN comp_id
   FROM CLASS smartdenserank.RemoveOffset;

insert into competitions values (1,1,0);

insert into competition_bands 
(comp_id 
    ,min_rank
    ,max_rank
    ,display_name)
values
(1,1,2,'Platinum');

insert into competition_bands 
(comp_id 
    ,min_rank
    ,max_rank
    ,display_name)
values
(1,3,6,'Gold');

insert into competition_bands 
(comp_id 
    ,min_rank
    ,max_rank
    ,display_name)
values
(1,7,11,'Silver');



insert into competition_bands 
(comp_id 
    ,min_rank
    ,max_rank
    ,display_name)
values
(1,12,50000000,'Bronze');




    
