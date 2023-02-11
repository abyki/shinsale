delete from deal_status a using deal_status b where a=b and a.ctid < b.ctid;

drop view if EXISTS w1 cascade;
create view w1 as 
SELECT DISTINCT id,
	timestamp,
    status,
    DENSE_RANK() over (partition by id order by timestamp) as rank,
    lead(timestamp) over (partition by id order by timestamp) as next_status
FROM deal_status
order by id, timestamp;

with w2 as(
select *, next_status - timestamp as diff, (next_status - timestamp)::FLOAT / 60 as diff_minuts
from w1
order by id, timestamp)

select status, ROUND(avg(diff_minuts)::numeric, 1) as time_in_minutes
from w2
group by status
having avg(diff_minuts) is not NULL