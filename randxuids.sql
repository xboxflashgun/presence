CREATE OR REPLACE FUNCTION getrandxuids2(div int, totauth int) RETURNS text 
VOLATILE AS $$
DECLARE

	xuidlist bigint[];
	xuid bigint;
	xuidstr text;

BEGIN
	
	FOR i in 0..1096 LOOP
		
		-- 0x10000000 + 0x1000000000 = 68987912192 = 263168 * 262144
		-- xuid := (((random() * 263168)::int8 * 262143 + (random() * 262144)::int8) / totauth)::bigint * totauth + div;

		xuid := ((('x'||encode(gen_random_bytes(8),'hex'))::bit(64)::int8 % 68987912192) / totauth)::bigint * totauth + div;

		-- 0x10000000 = 268435456, 0x1f000000000 - 0x10000000 = 2130035343360
		if xuid >= 268435456 then 
			xuid := xuid + 2130035343360; 
		end if;

		xuid := xuid + 0x9000000000000;

		xuidlist[i] := xuid;

	end loop;

	select 
		string_agg(foo.x::text,',') 
	from (
		select x from unnest(xuidlist) x) as foo 
	left join gamers on (gamers.xuid = x) 
	left join xuids0 on (xuids0.xuid = x)
	left join xuids1 on (xuids1.xuid = x)
	where gamers.xuid is null 
	and xuids0.xuid is null
	and xuids1.xuid is null
	into xuidstr;

	-- raise notice 'number of commas: %', (length(xuidstr)-16)/17 +1;

	return '{"level":"all","users":[' || xuidstr || ']}';

END
$$ LANGUAGE plpgsql;


