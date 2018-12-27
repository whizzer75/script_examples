#!/usr/bin/perl

use strict;
use warnings;

use DBI;

# Connect to storage capacity usage database
my $dbh = DBI->connect("dbi:Pg:dbname=xxxxxxx;host=192.168.0.10","user","secretpassword")
        or die "No DB connection\n";

my ($day,$month,$year,$last_day) = get_date_info();
my $rec_date = "$year-$month-$day";

# The below expect scripts log into a PURE array using key based SSH authentication
# and collect usage data
my %login = (
	PURE1 => '/home/whizzer/billing_new/collect_scripts/purevol_list.exp',
	PURE2 => '/home/whizzer/billing_new/collect_scripts/purevol_list2.exp',
);

my $sth = $dbh->prepare("INSERT INTO obj_temp (created,device,name,units,function_code) VALUES(current_timestamp,?,?,?,?)");

my %result;

# Loop through and run each of the scripts in %login
foreach my $device ( keys %login ) {
	my $script = $login{$device};
	# Capture results from each script and prepare data in %result
	# for insertion in the storage database
	foreach my $line (`$script`) {
		next unless $line =~ /\S+\s+\d\S+\s+-\s+\d\S+\s+\S+\s+\S+\s+\S+/;
		my @record = split(/\s+/, $line);
		my ($name,$size,$source,$date,$time,$tz,$serial) = @record;
		my ($c,$u) = $size =~ /(\d+)(\S+)/;
		my $gb;
		if ( $u eq 'G' ) {
			$gb = $c;
		} elsif ( $u eq 'T' ) {
			$gb = $c * 1024;
		}
		my @fields = ($device,$name,$gb,'ST60');
		#print "INSERT ", join('|',@fields), "\n";
		push @{$result{$device}}, \@fields;
	}
}

# Loop over %result and insert rows in storage database
foreach my $device (keys %result) {
    sql_delete($device);
    foreach my $r (@{$result{$device}}) {
        #print "INSERT ", join('|',@$r), "\n";
        my $sth = $dbh->prepare('INSERT INTO obj_temp (created,device,name,units,function_code) VALUES(current_timestamp,?,?,?,?)');
        $sth->execute(@$r);
    }
}

sub get_date_info {
    my ($day,$month,$year) = (localtime(time))[3,4,5];
    $year += 1900;
    $month += 1;

    use Date::Simple qw/days_in_month/;
    my $last_day = days_in_month($year, $month);
    return ($day,$month,$year,$last_day);
}

sub sql_delete {
    my $device = shift @_;
    my $sth = $dbh->prepare("delete from obj_temp where device = ?");
    $sth->execute($device);
}
