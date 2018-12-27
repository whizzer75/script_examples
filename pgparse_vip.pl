#!/usr/bin/perl

use strict;
use warnings;

use DBI;
my $mydb1 = DBI->connect("dbi:Pg:dbname=xxxxxxx;host=192.168.0.10","user","secretpassword")
	or die "No DB connection\n";

my ($sec,$min,$hour,$day,$month,$year,$last_day) = get_date_info();
my $rec_date = "$year-$month-$day";
my $rec_time = "$hour:$min:$sec";

update_vip_temp();

add_vip();

end_vip();

change_vip();

#############################################################################################

sub update_vip_temp {
    my $dsn = 'DBI:Sybase:server=VIP';
    my $vipdb = DBI->connect($dsn, 'vipuser', 'vippassword') or
        die "\nCan't connect to $dsn: $DBI::errstr\n";
    $vipdb->do('use VIP');
    my $query = qq|SELECT
          [KEY],
          [Description],
          [FMSCodes] AS 'Function Code',
          [Units],
          [TotalCost] AS 'Amount',
          GOTBillingNumber,
          CONVERT(CHAR(10), [StartDate], 101) as 'StartDate',
          CONVERT(CHAR(10), [EndDate], 101)   as 'EndDate',
          '' AS 'Comments'
    FROM
          vw_SpreadSheetBilling
    WHERE
        (
			FMSCodes like 'UX%' or FMSCodes like 'WN%' or FMSCodes like 'OR%' or FMSCodes like 'SQ%' or FMSCodes like 'ST%' or FMSCodes like 'VD%'
			or FMSCodes like 'LX%'
			or GOTBillingNumber = '15SER110' or GOTBillingNumber = '15SER310' or GOTBillingNumber like '15SER13%' or GOTBillingNumber like '15SER330'
		) 
 		AND 
		( 
			EndDate IS NULL
        	OR  DATEADD(D, 0, DATEDIFF(D, 0, EndDate)) >= DATEADD(D, -30, DATEDIFF(D, 0, GETDATE()) )
		)
    |;

    my $vipprod_selectnew = $vipdb->prepare ($query) or die "prepare failed\n";
    $vipprod_selectnew->execute() or die "unable to execute query $query error $DBI::errstr";

	print "Refreshing vip_temp table with new data from VIP production\n";
	$mydb1->do('DELETE FROM vip_temp');
	my $vip_temp_ins = $mydb1->prepare(
		'INSERT INTO vip_temp (created,key,description,function_code,units,amount,account,start_date,end_date,comments)
		VALUES (current_timestamp,?,?,?,?,?,?,?,?,?)'
	);
	my $count = 0;

	$mydb1->begin_work;

    while ( my @row = $vipprod_selectnew->fetchrow_array ) {
		$row[3] = int($row[3]);
		$row[1] =~ s/\r//g;
		$row[1] =~ s/\n//g;
		$row[1] =~ s/\t/ /g;
		chomp($row[1]);
		#print "Insert viptemp: $rec_date,@row\n";
		$vip_temp_ins->execute(@row);
		$count++;
	}
	#my $units = 1400;
	#my $amount = $units * 1000;
	#my @test = ('fake key','fake description','FK60',$units,$amount,'01234567','05/07/2018',undef,undef);
	#$vip_temp_ins->execute(@test);

	print "Found $count current records in VIP\n";

	$mydb1->commit;
}

sub add_vip {
	my $vip_temp_selectnew = $mydb1->prepare(
		"SELECT vip_temp.key,vip_temp.description,vip_temp.function_code,
			vip_temp.units,vip_temp.amount,vip_temp.account,vip_temp.start_date,
			vip_temp.end_date,vip_temp.comments
		FROM vip_temp LEFT OUTER JOIN vip
		ON vip_temp.key = vip.key
		WHERE vip.key IS NULL"
	);
	$vip_temp_selectnew->execute();

	my $myvip_ins = $mydb1->prepare(
		'INSERT INTO vip (created,key,description,function_code,units,amount,account,start_date,end_date,comments)
		VALUES (current_timestamp,?,?,?,?,?,?,?,?,?)'
	);
	my $myvip_history_ins = $mydb1->prepare(
		'INSERT INTO vip_history (id,created,key,description,function_code,units,amount,account,start_date,end_date,comments,rec_type)
		VALUES (?,current_timestamp,?,?,?,?,?,?,?,?,?,?)'
	);

	$mydb1->begin_work;

	my $count = 0;
	while (my @row = $vip_temp_selectnew->fetchrow_array) {
		print 'INSERT: ', join('|',map { defined($_) ? $_ : ''} @row ), "\n";
		$myvip_ins->execute(@row);
		my $id = $mydb1->selectrow_array('SELECT lastval()');
		$myvip_history_ins->execute($id,@row,'NEW');
		$count++;
	}
	
	print "Inserted $count new records from VIP production\n";

	$mydb1->commit;
}

sub end_vip {
	my $vip_end_old = $mydb1->prepare(
		"SELECT vip.id,vip.key,vip.description,vip.function_code,
			vip.units,vip.amount,vip.account,vip.start_date,vip.end_date,vip.comments
		FROM vip LEFT OUTER JOIN vip_temp
		ON vip.key = vip_temp.key
		WHERE vip_temp.key IS NULL"
	);
	my $myvip_history_ins = $mydb1->prepare(
        'INSERT INTO vip_history (id,created,key,description,function_code,units,amount,account,start_date,end_date,comments,rec_type)
        VALUES (?,current_timestamp,?,?,?,?,?,?,?,?,?,?)'
    );
	$vip_end_old->execute();

	$mydb1->begin_work;

	my $count = 0;
	while (my @row = $vip_end_old->fetchrow_array) {
		print 'END: ', join('|',map { defined($_) ? $_ : '' } @row ), "\n";
		my $id = shift @row;
		$myvip_history_ins->execute($id,@row,'END');
		$mydb1->do("DELETE FROM vip WHERE id=?",undef,$id);
		$count++;
	}
	print "Removed $count expired records from my VIP db.\n";

	$mydb1->commit;
}

sub change_vip {
	my $vip_temp_changes = $mydb1->prepare(
		"SELECT vip.id,vip_temp.key,vip_temp.description,vip_temp.function_code,
			vip_temp.units,vip_temp.amount,vip_temp.account,vip_temp.start_date,
			vip_temp.end_date,vip_temp.comments
		FROM vip_temp INNER JOIN vip
		ON vip_temp.key = vip.key
		WHERE ( 
			vip_temp.description <> vip.description OR vip_temp.function_code <> vip.function_code
			OR vip_temp.units <> vip.units OR vip_temp.amount <> vip.amount OR vip_temp.account <> vip.account
			OR vip_temp.start_date <> vip.start_date OR vip_temp.end_date <> vip.end_date
			OR vip_temp.comments <> vip.comments )
		OR (
			vip_temp.end_date IS NOT NULL AND vip.end_date IS NULL )"
	);
	$vip_temp_changes->execute();
	my $vip_history_ins = $mydb1->prepare(
		'INSERT INTO vip_history (id,created,key,description,function_code,units,amount,account,start_date,end_date,comments,rec_type)
		VALUES (?,current_timestamp,?,?,?,?,?,?,?,?,?,?)'
	);
	my $vip_update = $mydb1->prepare('UPDATE vip SET key=?, description=?, function_code=?, units=?, amount=?, account=?, start_date=?, end_date=?, comments=? WHERE id=?');

	$mydb1->begin_work;

	my $count = 0;
	while (my @row = $vip_temp_changes->fetchrow_array) {
		my $id = shift @row;
		print 'UPDATE: ', join('|',map { defined($_) ? $_ : '' } @row ), "\n";
		$vip_history_ins->execute($id,@row,'CHANGED');
		$vip_update->execute(@row,$id);
		$count++;
	}
	print "Changed $count records to match VIP production\n";

	$mydb1->commit;
}

sub get_date_info {
    my ($sec,$min,$hour,$day,$month,$year) = (localtime(time))[0,1,2,3,4,5];
    $year += 1900;
    $month += 1;

    use Date::Simple qw/days_in_month/;
    my $last_day = days_in_month($year, $month);
    return ($sec,$min,$hour,$day,$month,$year,$last_day);
}

