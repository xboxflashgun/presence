#!/usr/bin/perl -w

use Encode;
use Cpanel::JSON::XS;
use DBI;
use Time::HiRes qw( sleep );
use Fcntl qw(:flock);
use Proc::ProcessTable;
use Try::Tiny;

use lib '.';
use Xboxnew;

if(@ARGV != 1)	{

	print "\n  usage: $0 [div,total]\n\n";
	exit 0;

}

my ($div, $totauth) = split ",", $ARGV[0];

$|++;

my %grainer;

my $dbh = DBI->connect("dbi:Pg:dbname=global;port=6432") || die;
$dbh->do("insert into progstat values(now(), $$, $div, $div, 'localescan')");
$dbh->disconnect;

my $xbl = Xboxnew->new($div);
$dbh = $xbl->dbh;

my %countries;
my %langs;

my ($clips, $shots, $calls) = (0, 0, 0);

foreach $c ($dbh->selectall_array("select countryid,country from countries"))      {

	my ($cid, $name) = ($c->[0], $c->[1]);
	$countries{$name} = $cid;

}

foreach $l ($dbh->selectall_array("select langid,lang from languages")) {

	my ($lid, $name) = ($l->[0], $l->[1]);
	$langs{$name} = $lid;

}

my $time = time + 600;
my $usercnt = 0;

while( ($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] > 0)	{

	$all = $xbl->getall("select xuid from xuids1 where xuid % $totauth = $div order by scanned nulls first limit 1000");

	foreach $x (@$all)	{

		grain("shots");

		$xuid = $x->[0];

		$xbl->dbh->do("update xuids1 set scanned=now() where xuid=?", undef, $xuid);

		$dbh->do("update progstat set uptime=now() where pid=?", undef, $$);

		my ($json1, $json2);

		try {
			$json1 = decode_json($xbl->get(
				"https://screenshotsmetadata.xboxlive.com/users/xuid($xuid)/screenshots?maxItems=100&continuationToken="));
		} catch {};

		try {
			$json2 = decode_json($xbl->get(
				"https://gameclipsmetadata.xboxlive.com/users/xuid($xuid)/clips?maxItems=100&continuationToken="));
		} catch {};

		$calls++;

		my ($shotlocale, $shotdate, $cliplocale, $clipdate);

		if(defined($json1))	{
		
			foreach $shot (@{$json1->{'screenshots'}})	{

				my $locale = $shot->{'screenshotLocale'};
				next if not defined $locale;

				$shotlocale = $locale;
				$shotdate = $shot->{"dateTaken"};
				last;

			}

		}

		last if( ($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] < 1);

		if(defined($json2))	{
		
			foreach $clip (@{$json2->{'gameClips'}})		{

				my $locale = $clip->{'gameClipLocale'};
				next if not defined $locale;

				$cliplocale = $locale;
				$clipdate = $clip->{'dateRecorded'};
				last;

			}
	
		}

		my $locale;

		next if not defined $cliplocale and not defined $shotlocale;

		$shots++ if defined $shotlocale;
		$clips++ if defined $cliplocale;

		if( not defined $cliplocale )	{
			$locale = $shotlocale;
		} elsif( not defined $shotlocale )	{
			$locale = $cliplocale;
		} elsif( $clipdate gt $shotdate)	{
			$locale = $cliplocale;
		} else	{
			$locale = $shotlocale;
		}

		$locale =~ /(\w\w)-(\w\w)/;
		my ($langid, $countryid) = ($langs{$1}, $countries{$2});

		die "New country/lang: $2/$1" if not defined $langid or not defined $countryid;

		$dbh->begin_work;
		$dbh->do('insert into gamers(xuid,langid,countryid) values($1,$2,$3) on conflict(xuid) do nothing', undef, $xuid, $langid, $countryid);
		$dbh->do('delete from xuids1 where xuid=$1', undef, $xuid);
		$dbh->commit;
		$usercnt++;

		if(time > $time)	{

			$time = time + 600;
			print "Calls: $calls, Clips: $clips, Shots: $shots in a minute for $div\n";
			$dbh->do('insert into perflog(prog,prestime,xuids,secs,num) values($1,now(),$2,$3,$4)', undef, 'localescan', $usercnt, 600, $div);
			$usercnt = 0;

		}

	}

}

print "Graceful exit\n";

$dbh->do("delete from progstat where pid=$$");
$xbl->closedbh;


sub grain   {

	my $req = shift;

	my $sussecs = 30.5;
	my $susreqs = 300;

	push(@{$grainer{$req}}, time);

	if( @{$grainer{$req}} > $susreqs) {

		my $t = shift @{$grainer{$req}};
		if( time - $t <= $sussecs )  {

			my $sl = $sussecs - (time - $t) + 0.01;
			sleep $sl;

		}

	} else {

		sleep $sussecs / $susreqs;

	}

}

