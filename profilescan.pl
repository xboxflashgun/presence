#!/usr/bin/perl -w

use Cpanel::JSON::XS;
use Encode;
use DBI;
use Time::HiRes qw( sleep );
use Try::Tiny;
use Clone 'clone';
use POSIX;
use Data::Dumper;

use lib '.';
use Xboxnew;

if(@ARGV != 1)  {

    print "\n  usage: $0 [div,total]\n\n";
    exit 0;

}
 
my ($div, $totauth) = split ",", $ARGV[0];

my $xbl = Xboxnew->new($div);
my $dbh = $xbl->dbh;
$dbh->do("insert into progstat values(now(), $$, $div, $totauth, 'profilescan')");

my $coder = Cpanel::JSON::XS->new->allow_nonref->allow_blessed;
my %grainer;

# my @xuids = ( 2535473458706799, 2533275027095460, 2535438277624868, 2533274853287957, 2706851026034373 );
# process_batch(\@xuids);
# exit;

my $sttime = time + rand(600);

while(($dbh->selectrow_array("select pid from progstat where prog='runner'"))[0] > 0)	{

	if(time > $sttime)	{
	
		$dbh->do('
			insert into profiles(xuid) 
				select 
					xuid 
				from gamers 
				left join profiles using(xuid) 
				where 
					xuid % $1 = $2 
					and profiles.xuid is null 
			on conflict do nothing', undef, $totauth, $div);

		$sttime = time + 84600;			# once a day

	}

	grain('profile');

	my @xuids = map { $_->[0] } $dbh->selectall_array('select xuid from profiles where xuid % $1 = $2 order by scanned nulls first limit 15', undef, 
	$totauth, $div);
	process_batch(\@xuids);
	$dbh->do('update progstat set uptime=now() where pid=$1', undef, $$);

}

$dbh->do("delete from progstat where pid=$$");
$xbl->closedbh;
print "Graceful exit";


sub process_batch {

	my $xuidlist = shift;
	my @settings = qw /Gamerscore Gamertag RealName Bio TenureLevel Location GameDisplayPicRaw AccountTier XboxOneRep ModernGamertag ModernGamertagSuffix uniqueModernGamertag Watermarks RealNameOverride HasGamePass/;

	my $req = {
		'userIds' => $xuidlist,
		'settings' => \@settings
	};

	my $res = $xbl->post("https://profile.xboxlive.com/users/batch/profile/settings", 3, encode_json($req));

	my $json = decode_json($res);
	foreach $p (@{$json->{profileUsers}}) {

		my $xuid = $p->{id};
		my %sets;

		foreach $s (@{$p->{settings}}) {

			$sets{$s->{id}} = $s->{value};

		}

		my $gt = $sets{Gamertag};
		delete $sets{Gamertag};

		$dbh->do('insert into profiles(xuid,scanned,gt,profile) values($1,now(),$2,$3) on conflict(xuid) do update
			set scanned=now(),gt=$2,profile=$3', undef, $xuid, $gt, $coder->encode(\%sets));

	}

}


sub grain   {

	my $req = shift;

	my $sussecs = 300.5;
	my $susreqs = 30;

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

