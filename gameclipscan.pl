#!/usr/bin/perl -w

use Cpanel::JSON::XS;
use Encode;
use DBI;
use Time::HiRes qw( sleep );
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
$dbh->do("insert into progstat values(now(), $$, $div, $div, 'gameclipscan')");
$dbh->disconnect;

my $xbl = Xboxnew->new($div);
$dbh = $xbl->dbh;

my %countries;
my %langs;

foreach $c ($dbh->selectall_array("select countryid,country from countries"))	{

	my ($cid, $name) = ($c->[0], $c->[1]);
	$countries{$name} = $cid;

}

foreach $l ($dbh->selectall_array("select langid,lang from languages"))	{

	my ($lid, $name) = ($l->[0], $l->[1]);
	$langs{$name} = $lid;

}

while( ($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] > 0)	{

	my $titleid = $xbl->getall("select titleid from games where titleid % $totauth = $div order by clipscan nulls first limit 500");
	my $megacnt = 0;
	my $sttime = time;

	foreach $t (@$titleid)	{

		my $titleid = $t->[0];

		my $json;

		last if( ($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] < 1);

		try {
			$json = decode_json($xbl->get("https://gameclipsmetadata.xboxlive.com/public/titles/$titleid/clips?maxItems=100"));
		} catch {};

		$xbl->do("update games set clipscan=now() where titleid=$titleid");

		grain('game');

		next if not defined $json;

		my $cnt = 0;

		$xbl->do("begin");
		foreach $clip (@{$json->{'gameClips'}})	{

			$cnt += $dbh->do('insert into xuids0(xuid) values($1) on conflict(xuid) do nothing', undef, $clip->{'xuid'})
				if defined $clip->{'xuid'};

		}
		$xbl->do("commit");

		$dbh->do("update progstat set uptime=now() where pid=$$");

		$megacnt += $cnt;

	}

	print "$megacnt gamers added\n";
	$dbh->do('insert into perflog(prog,prestime,xuids,secs,num) values($1,now(),$2,$3,$4)', undef, 'gameclipscan', $megacnt, time-$sttime, $div);

}

print "Graceful exit\n";

$dbh->do("delete from progstat where pid=$$");
$xbl->closedbh;


sub grain   {

	my $req = shift;

	my $sussecs = 300.5;
	my $susreqs = 30;

	push(@{$grainer{$req}}, time);

	if( @{$grainer{$req}} >= $susreqs) {

		my $t = shift @{$grainer{$req}};
		if( time - $t <= $sussecs )  {

			my $sl = $sussecs - (time - $t) + 0.1;
			sleep $sl;

		}

	} else {

		sleep $sussecs / $susreqs;

	}

}

