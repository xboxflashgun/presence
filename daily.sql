--
update gamers 
	set lastutime=tab.utime 
from (
	select 
		xuid,
		max(utime) as utime 
	from presence 
	where 
		utime >= (select max(lastutime) from gamers) 
		and utime < extract(epoch from now())::int group by 1
) tab 
where 
	gamers.xuid=tab.xuid;
