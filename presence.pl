#!/usr/bin/perl -w

use Cpanel::JSON::XS;
use Encode;
use DBI;
use Time::HiRes qw( sleep );
use Try::Tiny;
use Clone 'clone';
use POSIX;

use lib '.';
use Xboxnew;

if(@ARGV != 1)	{

	print "\n  usage: $0 [div,total]\n\n";
	exit 0;

}

my %stoplist;	# stoplist{titleid} = 1 if need to skip

my ($div, $totauth) = split ",", $ARGV[0];

$|++;

my %grainer;

my %u_game;
my %pres;	# $pres{$xuid}{$titleid} = utime;
my %delcand;	# $delcand{$xuid}{$titleid} = utime;

my $dbh = DBI->connect("dbi:Pg:dbname=global;port=6432") || die;	# local DB
$dbh->do("insert into progstat values(now(), $$, $div, $div, 'presence')");
$dbh->disconnect;

my $xbl = Xboxnew->new($div);
$dbh = $xbl->dbh;

foreach $st ( $dbh->selectall_array("select titleid from games where not isgame or isgame is null") ) {

	$stoplist{$st->[0]} = 1;

}

my %devices;
map { $devices{$_->[1]} = $_->[0] } $dbh->selectall_array("select devid,devname from devices");

print "Starting $$\n";

print "Clean-up after last exit\n";

my $half = ($dbh->selectrow_array("with tab as (select secs from perflog where prog='presence' order by prestime desc limit 3) select avg(secs)/2 as half from tab"))[0];

my $maxutime = ($dbh->selectrow_array("
	select max(utime) from presence where xuid % $totauth = $div and utime > extract(epoch from now()-interval'1 week')::int"))[0];
my $ups = $dbh->do("

	update presence 
	set secs = $maxutime - utime + $half 
	where 
		xuid % $totauth = $div 
		and secs is null 
		and utime > extract(epoch from now()-interval'1 week')::int"

) if defined $maxutime;

print "Cleared $ups records\n" if defined $maxutime;

my $stop1 = ($dbh->selectrow_array("select extract(epoch from date_trunc('week',now()+interval '1 week'))::int"))[0];
	# restart at the end of the week

my $stop2 = ($dbh->selectrow_array("select extract(epoch from date_trunc('month',now()+interval '1 month'))::int"))[0];
	# restart at the end of the month

my $stoptime = ($stop1 < $stop2) ? $stop1 : $stop2;

while( ($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] > 0)	{

	my $num = 0;
	my $sttime = time;
	my @all = map { $_->[0] } @{$xbl->getall("select xuid from gamers where xuid % $totauth = $div")};

	my $total = scalar(@all);

	%delcand = %{ clone( \%pres ) };

	while( my @slice = splice(@all, 0, 1095) )	{

		my $res = process_batch( \@slice );	
		$res    = process_batch( \@slice )	if($res == 0);		# random fails
		$num += $res;

		last if( ($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] < 1);
		
		$dbh->do("update progstat set uptime=now() where pid=$$");

	}

	my $time = time;
	my $cycle = int(time - $sttime);
	
	$half = ($dbh->selectrow_array("with tab as (select secs from perflog where prog='presence' order by prestime desc limit 3) select avg(secs)/2 as half from tab"))[0];

	$dbh->do("begin");
	foreach $x (keys %delcand)	{

		foreach $t (keys %{$delcand{$x}})	{

			my $lastsecs = int($time - $cycle/2+35);
			$dbh->do("update presence set secs=?-utime+$half where xuid=? and titleid=? and utime=?", undef, $lastsecs, $x, $t, $pres{$x}{$t});
			delete $pres{$x}{$t};

		}

	}
	$dbh->do("commit");


	$cycle = int(time - $sttime);
	print strftime("%c $num online, full cycle = $cycle secs for $total gamers\n", localtime); 
	$dbh->do('insert into perflog(num,xuids,secs,prestime,prog) values($1,$2,$3,now(),$4)', undef, $div, $total, $cycle, 'presence');

	if(time >= $stoptime)	{

		print "Restarting $$\n";
		$dbh->do("update progstat set uptime='2010-01-01' where pid=$$");
		$xbl->closedbh;
		exit;

	}

}

print "Graceful exit\n";

$dbh->do("delete from progstat where pid=$$");
$xbl->closedbh;


###############################################################################

sub process_batch	{

	my $xuidlist = shift;

	my $req = {
		'level' => 'all',
		'users' => $xuidlist
	};

	grain("pres");

	my $res = $xbl->post("https://userpresence.xboxlive.com/users/batch?level=all", 3, encode_json($req));
	my $json;

	try	{
		$json = decode_json($res);
	} catch	{
	};

	return 0 if not defined $json;

	my $num = 0;
	my $time = int(time);

	$dbh->do("begin");
	foreach $j (@$json)	{

		my $jstate = $j->{state};
		next if $jstate eq 'Offline';

		my $xuid = $j->{xuid};

		foreach $d (@{$j->{devices}})   {

			my $devid = $devices{$d->{type}};
			if( not defined $devid)	{

				my $devname = $d->{"type"};
				use Data::Dumper;
				print Dumper(\%devices);
				$dbh->do('insert into devices(devid,devname) values ( (select max(devid)+1 from devices), $1)', undef, $devname);
				die "Unknown device detected: $devname";
			
			}

			foreach $tit (@{$d->{"titles"}})        {

				my $titleid = $tit->{id};

				next	if defined $stoplist{$titleid};		# skip apps

				if(!defined($u_game{$titleid})) {

					my $name    = $tit->{name};
					$name = "titleid_$titleid" if(!defined($name) or $name eq '');

					$dbh->do("insert into games(titleid,name) values(?,?) on conflict do nothing", undef, $titleid, $name);
					$u_game{$titleid} = 1;

				}

				my $place   = $tit->{placement};
				my $state   = $tit->{state};

				next	if($jstate ne 'Online' or $place eq 'Background' or $state ne 'Active');	# skip offline/away/bg

				$num++;
			
				if( ! defined $pres{$xuid}{$titleid})	{

					$pres{$xuid}{$titleid} = $time;
					$dbh->do('insert into presence(xuid,titleid,utime,devid) values(?,?,?,?)', undef, $xuid, $titleid, $time, $devid);
	
				}

				delete $delcand{$xuid}{$titleid};

			}

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
			# print "     (suddenly sleeping $sl)\n";
			sleep abs($sl);

		}

	} else {

		sleep $sussecs / $susreqs;

	}

}

