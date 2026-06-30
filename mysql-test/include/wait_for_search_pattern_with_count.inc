
# Purpose:
#    Simple search with Perl for a pattern in some file that repeats the
#    search until a timeout occurs.  Bulk of search logic copied from
#    include/search_pattern.inc
#
#    The environment variables SEARCH_FILE and SEARCH_PATTERN must be set
#    before sourcing this routine.  The environment variable SEARCH_WAIT
#    may be set to specify the time to keep waiting for the desired
#    pattern to appear.  The default timeout is 60 seconds.
#
#    In case of
#    - SEARCH_FILE and/or SEARCH_PATTERN is not set
#    - SEARCH_FILE cannot be opened
#    the test will abort immediate.

perl;
    use strict;
    my @search_files =         glob($ENV{'SEARCH_FILE'})     or die "SEARCH_FILE not set";
    my $search_pattern=        $ENV{'SEARCH_PATTERN'}        or die "SEARCH_PATTERN not set";
    my $search_wait=           $ENV{'SEARCH_WAIT'}     || 60;
    my $search_lines=          $ENV{'SEARCH_LINES'}    || 1;

    my $flag= 0;
    my $counter= $search_wait;

    # We include the current line and 'n' previous lines in its context
    # as a FIFO queue i.e., oldest entries in the queue are removed when
    # new lines are read from the file.

    my $line;

    while ($counter)
    {
        foreach my $search_file (@search_files) {
            open(FILE, "< $search_file") or die("Unable to open '$search_file': $!\n");

            # Read the file line by line
            while ($line= <FILE>)
            {
                my $cur_lineno= $.;
                my $file_pos= tell(FILE);

                if ($line =~ /$search_pattern/)
                {
                    # Line matches the specified pattern
                    $flag= $flag + 1;
                }
            }
            close(FILE);
        }

        if ($flag == $search_lines)
        {
            print "Pattern \"$search_pattern\" found\n";
            last;
        }
        sleep 1;
        $counter--;
    }
    if ($flag != $search_lines)
    {
        print "Pattern \"$search_pattern\" not found\n";
    }

EOF
