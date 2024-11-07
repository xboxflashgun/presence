#!/usr/bin/perl -w

use Cpanel::JSON::XS;
use Encode;
use DBI;
use Time::HiRes qw( sleep );
use Try::Tiny;
use POSIX;
use bigint;
use Data::Dumper;

use lib '.';
use Xboxnew;

if(@ARGV != 1)  {

	print "\n  usage: $0 [div,total]\n\n";
	exit 0;

}

my ($div, $totauth) = split ",", $ARGV[0];
$|++;
my %grainer;

my $coder = Cpanel::JSON::XS->new->allow_nonref->allow_blessed;

my $dbh = DBI->connect("dbi:Pg:dbname=global;port=6432") || die;
$dbh->do("insert into progstat values(now(), $$, $div, $div, 'brutescan')");
$dbh->disconnect;

my $xbl = Xboxnew->new($div);
$dbh = $xbl->dbh;

print "Starting $$\n";

my ($num, $outof) = (0, 0);
my $time = time+600;

while(($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] > 0) {

	$num += process_batch();
	if(time > $time)	{

		print "$num out of $outof in 10 minutes added\n";
		$dbh->do('insert into perflog(prog,prestime,xuids,secs,num) values($1,now(),$2,$3,$4)', undef, 'brutescan', $num, 600, $div);
		$time = time + 600;
		($num, $outof) = (0, 0);

	}

	$dbh->do("update progstat set uptime=now() where pid=$$");

}

print "Graceful exit with $num\n";

$dbh->do("delete from progstat where pid=$$");
$xbl->closedbh;




#################################################################################
sub process_batch	{

	grain("brute");

	my $res = $xbl->post("https://userpresence.xboxlive.com/users/batch?level=all", 3,
		($dbh->selectrow_array('select getrandxuids($1,$2)', undef, $div, $totauth))[0]);

	my $json;

	try	{
		$json = decode_json($res);
	} catch	{
	};

	return 0 if not defined $json;

	my $num = 0;

	$dbh->do("begin");
	foreach $j (@$json)	{

		my $jstate = $j->{state};
		if( $jstate ne 'Offline' or defined($j->{lastSeen}))	{

			$num += $dbh->do('insert into xuids1(xuid) values($1) on conflict(xuid) do nothing', undef, $j->{xuid});
			$outof++;

		}

	}
	$dbh->do("commit");

	return $num;

}


sub grain   {

	my $req = shift;

	my $sussecs = 300.5;
	my $susreqs = 100;

	push(@{$grainer{$req}}, time);

	if( @{$grainer{$req}} >= $susreqs) {

		my $t = shift @{$grainer{$req}};
		if( time - $t <= $sussecs )  {

			my $sl = $sussecs - (time - $t) + 0.1;
			sleep abs($sl);

		}

	} else {

		sleep $sussecs / $susreqs;

	}

}

