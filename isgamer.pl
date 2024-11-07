#!/usr/bin/perl -w

use DBI;
use Encode;

$|++;

my $dig = DBI->connect("dbi:Pg:dbname=xbox;host=flash.xboxstat.ru") || die;
my $glo = DBI->connect("dbi:Pg:dbname=global") || die;

my @all = $dig->selectall_array("select titleid,type from games");
print "  Total games: ", scalar(@all), "\n";

my $cnt = 0;

foreach $t (@all)       {

	my $titleid = $t->[0];
	my $type = $t->[1];
	
	$type = 'Unknown' if not defined $type;

	my $isgame = ($type eq 'Game') ? 'true' : 'false';

	$cnt += $glo->do("update games set isgame='$isgame' where titleid=$titleid");

}



print "\r  Well done: $cnt updates\e[K\n\n";

# titleid_1414629511 and a kind
@all = $glo->selectall_array("select titleid from games where name like 'titleid_%' and isgame");

foreach $t (@all)	{

	my $titleid = $t->[0];
	my $name = ($dig->selectrow_array("select name from games where titleid=$titleid"))[0];

	if( ! defined($name) )	{

		$name = "Unknown in dignus";

	}

	if( $name =~ /^titleid_/ )	{

		print "$titleid: unknown\n";

	} else	{

		print encode('utf-8', "$titleid -> $name\n");
		$name = $glo->quote($name);
		$glo->do("update games set name=$name where titleid=$titleid");

	}

}


$dig->disconnect;
$glo->disconnect;


