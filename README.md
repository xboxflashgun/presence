# presence

# flow

"xuids0" - All xuids found with brutforce & friends/clubs
   |
   +---> lastscan.pl - check if xuid is/was online ---> "xuids1"
			|
			+---> localescan.pl	- get locale
					|
					+---> "gamers"



"gamers" -> presence
"gamers" -> playscan
"lastseen" -> heroscan

clipscan -> "xuids0"

"xuids0" - all offline xuids
	xuid

"xuids1" - all online xuids w/o locale
	xuid

"gamers" - online xuids with locale
	+lastutime - updated from presence

if lastutime < now() - interval '1 year'
	- move to "xuids0" (gone offline)


