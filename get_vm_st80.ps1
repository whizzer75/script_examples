# Substitute your vcenter server IP below
Connect-VIServer 192.168.0.10

# Loop through all powered on vms and sum storage use where storage 
# is a standard virtual disk (not RDM or other). If datastore includes
# "SSD" in the name, bill ST60 flash rate instead of ST80 standard
# storage rate
$all_vms = Get-VM | Where-Object { $_.PowerState -eq 'PoweredOn' } 
$sum = @()
foreach($vm in $all_vms) {
	$st80 = @()
    $st60 = @()
	$vm | Get-Harddisk | Foreach-Object {
		if ($_.DiskType -match "Flat") {
            if ($_.Filename -like "``[*SSD*``]*") {
                #$_.Filename + " ST60"
                $st60 += $_
            } else {
                #$_.Filename + " ST80"
                $st80 += $_
            }
		}
	}

	# Create an array of new objects with summarized data
    if ( ($st60 | Measure-Object -Sum -Property CapacityGB).Sum -gt 0 ) {
	   $obj = New-Object -TypeName PSObject
       $obj | Add-Member -MemberType NoteProperty -Name Device -Value "vmware-cdc"
	   $obj | Add-Member -MemberType NoteProperty -Name Name -Value $vm.Name
	   $obj | Add-Member -MemberType NoteProperty -Name CapacityGB -Value ($st60 | Measure-Object -Sum -Property CapacityGB).Sum
       $obj | Add-Member -MemberType NoteProperty -Name FunctionCode -Value "ST60"
	   $sum += $obj
    }
    if ( ($st80 | Measure-Object -Sum -Property CapacityGB).Sum -gt 0 ) {
       $obj = New-Object -TypeName PSObject
       $obj | Add-Member -MemberType NoteProperty -Name Device -Value "vmware-cdc"
	   $obj | Add-Member -MemberType NoteProperty -Name Name -Value $vm.Name
	   $obj | Add-Member -MemberType NoteProperty -Name CapacityGB -Value ($st80 | Measure-Object -Sum -Property CapacityGB).Sum
       $obj | Add-Member -MemberType NoteProperty -Name FunctionCode -Value "ST80"
	   $sum += $obj
    }
}

# Output report to ASCII CSV file
$sum | Export-CSV -Encoding ASCII -Path C:\bill\vmware_cdc_st80.csv
