load classes ../../smartdenserank.jar;


delete from ratings;

select * from ratings order by u_rank ;

exec ReportRank 1 1;
exec ReportRank 1 1;
exec ReportRank 1 1;
exec ReportRank 1 1;
exec ReportRank 1 1;
exec ReportRank 1 1;

exec ReportRank 1 2;
exec ReportRank 1 2;
exec ReportRank 1 2;

select * from ratings order by u_rank, score desc ;

exec ReportRank 1 3;

select * from ratings order by u_rank, score desc ;

exec ReportRank 1 3;

select * from ratings order by u_rank, score desc ;

exec ReportRank 1 3;

select * from ratings order by u_rank, score desc ;

exec ReportRank 1 3;

select * from ratings order by u_rank, score desc limit 20;

select sum(score) from ratings;

select * from competitions;
:q

