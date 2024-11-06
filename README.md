# presence

## flow
```
"xuids0" - All xuids found with friends/clubs & offline > 1 year
   |
   +---> lastscan.pl - check if xuid is/was online + brutscan ---> "xuids1"
			|
			+---> localescan.pl	- get locale
					|
					+---> "gamers"



"gamers" -> presence
"gamers" -> playscan
"lastseen" -> heroscan

gameclipscan -> "xuids0"

oldscan: "xuids0" -> "xuids1"

"xuids0" - all offline xuids
	xuid

"xuids1" - all online xuids w/o locale
	xuid

"gamers" - online xuids with locale
	+lastutime - updated from presence

if lastutime < now() - interval '1 year'
	- move to "xuids0" (gone offline)

```

## presence

```
presence:
 - measure the time for full cycle
	"perflog":
		timestamp
		xuids 	- processed
		secs	- spent
		num		- process number
```



