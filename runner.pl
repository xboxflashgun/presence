#!/usr/bin/perl -w

use DBI;

$|++;

my $dbh = DBI->connect("dbi:Pg:dbname=global;port=6432;user=runner;password=runner74") || die;	# local

my @progs = $dbh->selectall_array("select prog,num from progs order by prog desc");

$dbh->do("delete from progstat where prog='runner'");
$dbh->do("insert into progstat(pid,prog) values(1, 'runner')");

my $bans = ($dbh->selectrow_array("select count(*) from auth where banned"))[0];

print "Number of accs banned: $bans\n";
sleep 3;

foreach $p (@progs)	{

	my ($prog, $num) = ($p->[0], $p->[1]);

	print "    > ";
	system("killall $prog.pl");
	$dbh->do("delete from progstat where prog='$prog'");

	sleep(1);

	run_progs($prog, $num);

}

while(1)	{

	sleep 2;
	print "-------\e[K\n";
	my $rows = 1;
	my $tots = 0;

	foreach $p (@progs)	{

		next	if $p->[1] eq 0;

		$rows++;

		my $prog = $p->[0];
		my @stat = $dbh->selectall_array("
			select 
				avg(extract(epoch from now()-uptime)),
				max(extract(epoch from now()-uptime)),
			       	count(*)	
			from progstat 
			where prog='$prog'");

		if($stat[0]->[2] == 0)	{
			print sprintf("%14s: skipping, num=%2d\e[K\n", $prog, $stat[0]->[2]);
		} else	{
			print sprintf("%14s: avg=%9.2f max=%8.2f cnt=%3d (num = %2d)\e[K\n", $prog, $stat[0]->[0], $stat[0]->[1], $stat[0]->[2],$p->[1]);
		}

		$tots += $stat[0]->[2];

	}

	print "\e[${rows}A";
	
	if( ($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0])	{

		check_progs();

	} else	{

		print "\r > Waiting for $tots scripts to exit\e[K";
		last if($tots == 0);

	}

}

print "\n" x scalar(@progs);
$dbh->do("update progstat set pid=1 where prog='runner'");	# allow to run outside of runner
$dbh->disconnect;


sub check_progs	{

	my @lost = $dbh->selectall_array("
		select 
			prog,pid,authid,divider,num,0,'uname' 
		from progstat join progs using(prog) 
		where now()-uptime > interval '720 seconds'
	");

	return if scalar(@lost) == 0;

	print "\e[J";		# clear screen up to the end of display

	if(scalar(@lost) > 0)	{

		while($p = shift @lost)		{

			my ($prog,$pid,$authid,$div,$num,$banned,$uname) = (
				$p->[0],$p->[1],$p->[2],$p->[3],$p->[4],$p->[5],$p->[6]
			);

			next if $num eq 0;

			print "Old $prog.pl($pid) authid=$authid 0? ($uname), banned = $banned, killing\n";

			print "    > ";
			system("kill $pid");
			$dbh->do("delete from progstat where pid=$pid");

			sleep 3;

			if($banned)	{

				print "  > This authid is banned, so not restarting\n";
				return;

			}

			print "  > Restarting $prog.pl $div,$num\n";
			sleep 3;
			system("/usr/bin/nohup ./$prog.pl $div,$num >> logs/$prog.$div.log 2>&1 &");

		}

	}

}

sub run_progs	{

	my ($prog, $num) = @_;

	return	if $num eq 0;

	if( $prog eq 'presence' )	{	# create index

		# CREATE INDEX presnullind ON ONLY public.presence USING btree (((xuid % (84)::bigint))) WHERE (secs IS NULL)
		my $def = ($dbh->selectrow_array("select indexdef from pg_indexes where indexname='presnullind'"))[0];

		if($def =~ /xuid % \($num\)::bigint/)	{
			print "Index is up to date\n";
		} else {

			print "Creating index for presence: 1\n";
			$dbh->do("drop index xuidlistind");
			$dbh->do("create index xuidlistind on gamers((xuid % $num))");

			print "Creating index for presence: 2\n";
			$dbh->do("drop index presnullind");
			$dbh->do("create index presnullind on presence ((xuid % $num)) where secs is null");

		}

	}

	if( $prog eq 'lastscan')	{	# create lastscan index

		my $def = ($dbh->selectrow_array("select indexdef from pg_indexes where indexname='xuids0_last_idx'"))[0];

		# CREATE INDEX xuids0_last_idx ON public.xuids0 USING btree (((xuid % (42)::bigint)))
		if($def =~ /xuid % \($num\)::bigint/) {
			print "lastscan index is up to date\n";
		} else {

			print "Creating index for lastscan\n";
			$dbh->do("drop index xuids0_last_idx");
			$dbh->do("create index xuids0_last_idx on xuids0((xuid % ${num} ::bigint))");

		}

	}
	
	if( $prog eq 'friendscan')	{	# create index	- also used for presence!

		$dbh->do("drop index friendgamersidx");
		$dbh->do("create index friendgamersidx on gamers((xuid % $num), friendscan nulls first)");

	}

	if( $prog eq 'playscan')	{	# create index

		print "Creating index playscanidx\n";
		$dbh->do("drop index playscanidx");
		$dbh->do("create index playscanidx on gamers((xuid % $num), playscan nulls first) where countryid is not null");

	}

	if( $prog eq 'localescan')	{	# create index

		my $def = ($dbh->selectrow_array("select indexdef from pg_indexes where indexname='xuids1_locind'"))[0];

		# CREATE INDEX xuids1_locind ON public.xuids1 USING btree (((xuid % (42)::bigint)), scanned NULLS FIRST)
		if($def =~ /xuid % \($num\)::bigint/) {
			print "localescan index is up to date\n";
		} else {
			print "Creating index xuids1_locind\n";
			$dbh->do("drop index xuids1_locind");
			$dbh->do("create index xuids1_locind on xuids1 ((xuid % ${num} ::bigint), scanned nulls first)");

	}

	if( $prog eq 'memberscan')	{	# create index

		print "Creating index memberscanidx\n";
		$dbh->do("drop index memberscanidx");
		$dbh->do("create index memberscanidx on gamers((xuid % $num),clubscan nulls first) WHERE playscan IS NOT NULL");

	}

	my $div = 0;
	while($div != $num)	{

		print "Running ./$prog.pl $div,$num\n";
		unlink "logs/$prog.$div.log";
		sleep 1;
		system("/usr/bin/nohup ./$prog.pl $div,$num >> logs/$prog.$div.log 2>&1 &");
		$div++;

	}

}

