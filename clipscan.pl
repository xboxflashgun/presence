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
$dbh->do("insert into progstat values(now(), $$, $div, $div, 'clipscan')");
$dbh->disconnect;

my $xbl = Xboxnew->new($div);
$dbh = $xbl->dbh;

print "Starting $$\n";
my $totcnt = 0;

while(($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] > 0) {


	my $sttime = time;

	my @all = map { $_->[0] } @{$xbl->getall("
		select xuid 
		from xuids2 
		where 
			scanned < now()-interval '6 hours' 
			or scanned is null
		order by scanned nulls first 
		limit 1095
	")};
	my $total = scalar(@all);

	last if scalar(@all) == 0;

	my $num = 0;
	
	$num += process_batch( \@all );

	last if( ($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] < 1);
	$dbh->do("update progstat set uptime=now() where pid=$$");

	$dbh->do('insert into perflog(prog,prestime,xuids,secs,num) values($1,now(),$2,$3,$4)', undef, 'clipscan', $total, time-$sttime, $div);
	print "Full cycle for $total xuids finished in ", time-$sttime, " secs, moved $num xuids\n";
	$totcnt += $num;

}

print "Graceful exit with total $totcnt moved\n";

$dbh->do("delete from progstat where pid=$$");
$xbl->closedbh;




#################################################################################
sub process_batch	{

	my $xuidlist = shift;
	my $req = {
		'level' => 'all',
		'users' => $xuidlist
	};

	grain("clip");

	my $res = $xbl->post("https://userpresence.xboxlive.com/users/batch?level=all", 3, encode_json($req));
	my $json;

	try	{
		$json = decode_json($res);
	} catch	{
	};

	return 0 if not defined $json;

	my $num = 0;

	if(ref($json) eq 'HASH') {

		print "! Warn: check incorrect xuid numbers in xuids0\n";
		return 0;

	}

	foreach $j (@$json)	{

		my $jstate = $j->{state};
		$dbh->do("begin");
		if( $jstate ne 'Offline' or defined($j->{lastSeen}))	{

			$num += $dbh->do('
				insert into gamers(xuid,countryid,langid) 
					select 
						xuid,
						countryid,
						langid 
					from xuids2 
					where xuid=$1 
				on conflict(xuid) do nothing', undef, $j->{xuid});
			$dbh->do('delete from xuids2 where xuid=$1', undef, $j->{xuid});

		} else {

			$dbh->do('update xuids2 set scanned=now() where xuid=$1', undef, $j->{xuid});

		}
		$dbh->do("commit");

	}

	return $num;

}


sub grain   {

	my $req = shift;

	my $sussecs = 400.5;
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

