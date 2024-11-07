---

begin;
	
	create temp table t1 as
		select 
			now() as hdate,
			count(*) as gamers,
			count(*) filter (where lastutime>extract(epoch from now()-interval'1 week')::int) as weekgamers
		from gamers;

	create temp table t2 as 
		with tab as (
			select count(*) as xuids0,
			count(*) filter (where scanned is not null) as xuids0scanned 
		from xuids0
	) select * from tab;

	create temp table t3 as 
		with tab as (
			select count(*) as xuids1,
			count(*) filter (where scanned is not null) as xuids1scanned 
		from xuids1
	) select * from tab;

	create temp table t4 as
		select
			min(scanned) as xuids0min,
			max(scanned) as xuids0max
		from xuids0;

	create temp table t5 as
		select
			min(scanned) as xuids1min,
			max(scanned) as xuids1max
		from xuids1;

	insert into history2
		select 
			hdate,
			gamers,
			weekgamers,
			xuids0,
			xuids0scanned,
			xuids1,
			xuids1scanned,
			xuids0min,
			xuids0max,
			xuids1min,
			xuids1max
		from t1, t2, t3, t4, t5;

	drop table t1;
	drop table t2;
	drop table t3;
	drop table t4;
	drop table t5;

commit;

