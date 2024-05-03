package IBIS::EmpowerIT::Web_Util;

use strict;
use version; our $VERSION = qv('0.0.8');
use Data::Dumper;
use POSIX qw(strftime);
use Carp;
use base qw(Exporter);
use DBI;
use IBIS::DBI;
use File::stat;
use Net::SFTP;
use MCCS::Config;
use DateTime;
use File::Basename;

sub new {
    my ($class, %args) = @_;
    my $self = {};
    
    ## parse config information into the object
    my $g_cfg       = new MCCS::Config;
    ## print Dumper($g_cfg->empowerit);
    ## my ($host, $user, $passwd, $remote_dir, $base_dir);
    $self->{sftp_host}        = $g_cfg->empowerit->{host};
    $self->{sftp_remote_dir}  = $g_cfg->empowerit->{remote_dir};
    $self->{sftp_user}        = $g_cfg->empowerit->{username};
    $self->{sftp_password}    = $g_cfg->empowerit->{password};
    $self->{local_data_dir}   = $g_cfg->empowerit->{local_dir};
    $self->{filehistory}      = $g_cfg->empowerit->{filehistory};
    $self->{webserver_relative_dir}    = $g_cfg->empowerit->{webserver_relative_dir};

    bless $self, $class;
    return $self;
}


sub irdb_connect{
    my ($self) = @_;
    $self->{'dbh'} = IBIS::DBI->connect(dbname => 'irdb');
    return $self->{'dbh'};
}


sub print_empowerit_upload_form{
    my ($self, $filename) = @_;
    my $name = "EmpowerIT Upload Form";
    my    $form_buffer = qq{<table align='center'><form action="index.html" method="post" enctype="multipart/form-data" onsubmit="return validateForm()" >
    <tr><td>
     Upload a new file:
     <input name="userfile" type="file" class="button" >
    </td></tr>
     <tr><td></td></tr>
     <tr><td align='center'>
     <input type=submit name=submit value='submit'>        
    </td></tr> 
   </form> 
   </table>                                                   
    };
    return $form_buffer;
}


##print a blue line under 
sub print_blue_line{
    my ($self) = @_;
    my $buffer .= "<hr noshade>"; 
    return $buffer;
}


sub print_subtitle{
  my ($self, $msg) = @_;
    my $message = "<table class='subtitle'>";
    $message .= "<tr><td>".$msg."</td></tr></table>";
    return $message;
}


sub print_return_msg{
  my ($self, $msg) = @_;
    my $message = "<table class='return_msg'>";
    $message .= "<tr><td>".$msg."</td></tr></table>";
    return $message;
}

## return message in a box..

sub print_message_box{
    my ($self, $msg) = @_;
    my $message = "<table class='e832_table_nh'>";
    $message .= "<tr><td>".$msg."</td></tr></table>";
    return $message;
}


## parse information just on the files 
## may included information from the privious saved file
## save information onto a reference, same info for old or new files

sub parse_file_info{
    my ($self, $dir)= @_;
    
## default to local data dir path:
    $dir = $self->get_local_data_dir() unless $dir;
    
    my $file_list_ref = $self->get_file_list_from_directory($dir);
## parse information from the File::stat
    my $file_info_ref;
    foreach my $file(@$file_list_ref){
	my $filepath = $dir.'/'.$file; ## unix does not mind '//'
	my $fs_ary = (stat($filepath));
	my $size = $fs_ary->[7];
	my $mtime= $fs_ary->[9];
	my $ctime= $fs_ary->[10];
	my ($fmd_ctime) = $self->get_file_time($ctime);
	my ($fmd_mtime) = $self->get_file_time($mtime);
	## use filename + ctime to make a uniq key for file information
	my $comb_key = $file;##."_".$fmd_ctime;
	## save current file info:
	$file_info_ref->{$file}->{'size'} = $size;
	$file_info_ref->{$file}->{'fmd_ctime'} = $fmd_ctime;
	$file_info_ref->{$file}->{'fmd_mtime'} = $fmd_mtime;
	$file_info_ref->{$file}->{'file_name'} = $file;
	## the followings maybe from the saved data:
	$file_info_ref->{$file}->{'user'} ='';
	$file_info_ref->{$file}->{'send_status'} = '';
    }
    $self->sget_file_info($file_info_ref);## save information
    return $file_info_ref;
}

## setter and getter of file info reference
sub sget_file_info{
    my ($self, $file_info_ref) = @_;
    if($file_info_ref){
	$self->{'file_info_ref'} = $file_info_ref;
	return $self->{'file_info_ref'};
    }else{
	return $self->{'file_info_ref'};
    }
}

## this will give back the leaf file name, not the full path
sub get_file_list_from_directory{
    my ($self, $data_dir) = @_;
    my @file_list;
    opendir(DIR, $data_dir)|| die "can not open directory: $data_dir\n";
    while (my $file = readdir(DIR)) {
	next if ($file =~ m/^\./);
        push(@file_list, $file);
    }
    close DIR;
    return \@file_list;
}

sub print_empowerit_file_list{
    my ($self) = @_;
    
## get parsed file information 
    $self->parse_file_info();
    my $file_info_ref = $self->sget_file_info();
    my $local_data_dir =$self->get_webserver_relative_dir;
    my $f_his_ref = $self->read_history_file();

    my $name = "EmpowerIT Files To Be Sent";
    
## print them into a table form as data:
 
    my $form_buffer .= "<br>".$self->print_subtitle($name);
    $form_buffer .= 
	'<br><form action="index.html" method="post" enctype="multipart/form-data" onsubmit="return validateForm()" >
<table class="report_clean_tight_row" >
    <tr>
    <th >File Name</th>
    <th >File Time</th>
    <th >Size</th>
    <th >To Send</th> 
    </tr> ';
    foreach my $file(keys %$file_info_ref){
	my $send_status = $f_his_ref->{$file}->{'send_status'};
	if(!$send_status){
	    my $file_name   = $file_info_ref->{$file}->{'file_name'};	    
	    my $file_link   =  "<a href=\"".$local_data_dir.$file."\">".$file."</a>";
	    my $filetime    = $file_info_ref->{$file}->{'fmd_ctime'}; ## file create time
	    my $size        = $file_info_ref->{$file}->{'size'};	    
	    $form_buffer .=  qq{<tr><td>$file_link</td><td>$filetime</td><td><i>$size</i></td><td> <input type="checkbox" name="select_file" value="$file_name"></td></tr>};
	}
    }
    $form_buffer .= "</table><br>
      <table align='center'><tr><td align='center' vlign='middle'><input type=submit name=submit value='submit'></td></tr></table>
      </form>";
    return $form_buffer;
}

sub print_empowerit_report_list{
    my ($self, $data_dir) = @_;

    
## get parsed file information 
    $self->parse_file_info();
    my $file_info_ref = $self->sget_file_info();
    
## get history file information
    my $f_his_ref = $self->read_history_file();
    
## get local data directory path for creating links  
    my $local_data_dir = $self->get_webserver_relative_dir; 
    
## get form buffer:
    my $name = "EmpowerIT Files Sent Before";
    my $form_buffer .= "<br>".$self->print_subtitle($name);
    $form_buffer .= 
'<br><form action="index.html" method="post" enctype="multipart/form-data" onsubmit="return validateForm()" >
<table class="report_clean_tight_row" >
    <tr>
    <th >File Name</th>
    <th >File Time</th>
    <th >Size (bytes)</th>
    <th >Last Change</th> 
    <th >Last Sent</th>
    <th >Sent-by</th>
    <th >Select</th>
    </tr> ';

## put data into the form:
    foreach my $file(keys %$file_info_ref){
	my $send_status = $f_his_ref->{$file}->{'send_status'};
	if($send_status eq 'Y'){
	    my $file_name   = $file_info_ref->{$file}->{'file_name'};
	    my $file_link   =  "<a href=\"".$local_data_dir.$file_name."\">".$file_name."</a>";
	    my $fmd_ctime   = $file_info_ref->{$file}->{'fmd_ctime'}; ## file create time
	    my $size        = $file_info_ref->{$file}->{'size'};
	    my $fmd_mtime   = $file_info_ref->{$file}->{'fmd_mtime'}; ## last modify time 
	    
	    ## get other saved information from comb_key of filename and fmd_ctime
	    my $user        = $f_his_ref->{$file}->{'user'};
	    my $send_status = $f_his_ref->{$file}->{'send_status'};
	    my $send_time   = $f_his_ref->{$file}->{'send_time'};
	    ## get buffer 
	    $form_buffer .=  qq{
    <tr><td>$file_link</td>
    <td>$fmd_ctime</td>
    <td><i>$size</i></td>
    <td>$fmd_mtime</td>   
    <td>$send_time</td>
    <td>$user</td>
    <td><input type="checkbox" name="select_file" value="$file_name"></td>};
	}
    }
    $form_buffer .="</table><br>
    <table align='center' vlign='middle'>
    <tr><td>
        <input type=submit name=resend value='resend'>
      </td> 
      <td>
        <input type=submit name=delete value='delete'> 
      </td></tr></table>
      </form>";
    return $form_buffer;
}


## working on it...
sub get_file_history{
    my ($self, $file) = @_;
    
    return $self->{'filehistory'};
    
}


## use mtime to parse human readable time(mm-dd-yy), and file size(bytes).
sub get_file_time{
    my ($self, $mtime) = @_;
    use Time::localtime;
    my $root = (localtime($mtime));
    my ($min,$h,$d,$m,$y) =  ($root->[1],$root->[2],$root->[3],$root->[4],$root->[5]);
    $m = $m + 1;
    $y = $y + 1900;
    $min = sprintf("%02d",$min);
    $h = sprintf("%02d",$h);
    $d = sprintf("%02d",$d);
    $m = sprintf("%02d",$m);
    $y = sprintf("%04d",$y);
    my $sy = substr($y,2,2);
    my $filetime = $sy.$m.$d.$h.$min; ##detail info may be useful here  
    return $filetime;
}


## get current time string yymmddhhmm
sub get_current_time{
    my ($self) = @_;
    my $dt     = DateTime->now;
    my $ymd    = $dt->ymd;
    my $hour   = $dt->hour - 5; ## seems 5 hour earlier  # 0-23
    my $minute = $dt->minute;         # 0-59 - also min
    my $t_st = $ymd." ".$hour.$minute;
    return $t_st;
}


## some getters
sub get_sftp_user{
    my ($self) = @_;
    return $self->{sftp_user};

}

sub get_sftp_host{
    my ($self) = @_;
    return $self->{sftp_host};

}

sub get_sftp_remote_dir{
 my ($self) = @_;
    return $self->{sftp_remote_dir};

}

sub get_sftp_password{
 my ($self) = @_;
    return $self->{sftp_password};

}

sub get_local_data_dir{
    my ($self) = @_;
    return $self->{local_data_dir};
    
}

sub get_webserver_relative_dir{
    my ($self) = @_;
    return $self->{webserver_relative_dir};
}

sub send_empower_files{
    my ($self, $file_list) = @_;

## get attribute values:
    my $host = $self->get_sftp_host;
    my $remote_dir  = $self->get_sftp_remote_dir;
    my $user        = $self->get_sftp_user;
    my $passwd      = $self->get_sftp_password; 
    my $base_dir    = $self->get_local_data_dir;

   open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
   open STDOUT, '>>/dev/null' or die "Can't write to /dev/null: $!";
   open STDERR, '>>/dev/null' or die "Can't write to /dev/null: $!";


## empowerit has setup sftp without using password.., this is testing that...
##  my $sftp = Net::SFTP->new(
##	$host,
##	user     => $user
##	) || die " Can not connect to remote server: dev.empowerit.com... !\n";

## send over the list:
    my @file_sent;
    my @file_failed;
    my $result = 1;
    foreach my $file(@$file_list){
	my ($local_file, $remote_file);
	$local_file  = $base_dir.$file; 
	$remote_file = $remote_dir.$file;

###	my $scp_cmd ="scp $local_file  ".'mcxdata@dev.empowerit.com:'.$remote_file;
        my $scp_cmd ="scp $local_file  "."$user".'@'."$host".':'.$remote_file; 
	eval{
	    ## my $ftp_result = $sftp->put( $local_file, $remote_file);
	    system("$scp_cmd");
	};
	if($@){
	    warn("Failed to send file $file. $@");
	    $result = 0 if ($result);
	    push(@file_failed, $file);
	}else{
	    push(@file_sent, $file);
	}
    }
    ##$sftp = undef;
    return ($result,\@file_sent,\@file_failed);
}

## IBIS::EDI_Utils::send_empower_files
## get a list ref, return table buffer reference.
sub print_array_list_to_table{
    my ($self, $list, $element_name) = @_;
    ##print "You have following ele in your list:\n";
    my $table_buffer = qq{<table class="report_clean_tight_row"><tr><th >$element_name</th></tr> };
    foreach my $ele(@{$list}){
	$table_buffer .= "<tr><td>$ele</td></tr>";
    }
    $table_buffer .= "</table>";
    return \$table_buffer;
}

sub print_time_elapse{
    my ($self, $start_time) = @_;   
    use Time::HiRes qw(time);
    my $t0 = time;
    ##$t0 = $start_time;
    my $elapsed = time - $t0;
}


##input: list of files to record
##output: success or failure in saving info
sub save_file_info{
    my ($self, $file_list, $user) = @_;
    $self->parse_file_info();
    my $file_info_ref = $self->sget_file_info();
    my $filehistory   = $self->get_file_history();
    my $send_time = $self->get_current_time();

    ## save information into the file history:    
    open(OUT, ">>$filehistory");
    my $send_status = 'Y';
    
    ## writting from file_info_ref
    foreach my $file(@$file_list){
	if($file =~ m/(.*)[\\\/](.+)/){
	    $file = $2;  ## remove the full filepath in front of the leaf name
	}
	my $ctime = $file_info_ref->{$file}->{'fmd_ctime'};
	my $size  = $file_info_ref->{$file}->{'size'};
	my $mtime = $file_info_ref->{$file}->{'fmd_mtime'};
	my $str = "\n".$file.'|'.$ctime.'|'.$size.'|'.$mtime.'|'.$user.'|Y|'.$send_time."|";
	print OUT $str;
    }
    close OUT;    
    return $filehistory;

 
}

sub read_history_file{
    my ($self) = @_;
    my $filehistory   = $self->get_file_history();
    open(IN, "<$filehistory");
    my @line_ary = <IN>;
    close IN;    
     my $f_his_ref;   
    foreach my $line(@line_ary){
##filename|filectime|size|modtime|user|send_status|sendtime|
##email2.txt|1201230926|1028|1201230926|yuc|Y|test time|
	my @items = split(/\|/, $line);
	$f_his_ref->{$items[0]}->{file_name}   = $items[0];
	$f_his_ref->{$items[0]}->{fmd_ctime}   = $items[1];
	$f_his_ref->{$items[0]}->{size}        = $items[2];
	$f_his_ref->{$items[0]}->{fmd_mtime}   = $items[3];
	$f_his_ref->{$items[0]}->{user}        = $items[4];
	$f_his_ref->{$items[0]}->{send_status} = $items[5];
	$f_his_ref->{$items[0]}->{send_time}   = $items[6];
    }
    return $f_his_ref;  
}


sub add_file_history{
    my ($self, $fn_ft, $file_info) = @_;
}

## delete files in data directory. (list has leafy names only)...
sub delete_empowerit_files{
    my ($self, $list_to_delete) = @_;
    my $local_data_dir = $self->get_local_data_dir();
    my $delete_result = 1;
    foreach my $file(@$list_to_delete){
	my $f_fname = $local_data_dir.'/'.$file;
	my $cmd = "rm $f_fname";
	eval{
	    system($cmd);
	};
	if($@){
	    $delete_result = 0;
	}
    }
    return $delete_result;
}

1;

    
