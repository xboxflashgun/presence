
-- psql -f set-up-num.sql

-- Wed 13 Dec 21:59:57 UTC 2023   num=5
--

update progs set num =  0 where prog =   'friendscan';
update progs set num = 42 where prog =   'localescan';
update progs set num =  0 where prog =     'playscan';
update progs set num =  0 where prog =     'clubscan';
update progs set num =  0 where prog = 'gameclipscan';
update progs set num =  0 where prog =     'heroscan';
update progs set num =  0 where prog =   'memberscan';
update progs set num = 84 where prog =     'presence';
update progs set num =  1 where prog =     'lastscan';
update progs set num = 83 where prog =    'brutescan';

select * from progs order by num;

select sum(num) from progs;

--

