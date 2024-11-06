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
$dbh->do("insert into progstat values(now(), $$, $div, $div, 'lastscan')");
$dbh->disconnect;

my $xbl = Xboxnew->new($div);
$dbh = $xbl->dbh;

print "Starting $$\n";
my $totcnt = 0;

while(($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] > 0) {


	my $sttime = time;

	my @all = map { $_->[0] } @{$xbl->getall("select xuid from xuids0 where xuid % $totauth = $div order by scanned nulls first limit 1095*50")};
	my $total = scalar(@all);

	my $num = 0;
	
	while( my @slice = splice(@all, 0, 1095) )  {

		$num += process_batch( \@slice );

		last if( ($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] < 1);
		$dbh->do("update progstat set uptime=now() where pid=$$");

	}

	print "Full cycle for $total xuids finished in ", time-$sttime, " secs\n";
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

	grain("last");

	my $res = $xbl->post("https://userpresence.xboxlive.com/users/batch?level=all", 3, encode_json($req));
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
			$dbh->do('delete from xuids0 where xuid=$1', undef, $j->{xuid});

		} else {

			$dbh->do('update xuids0 set scanned=now() where xuid=$1', undef, $j->{xuid});

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

