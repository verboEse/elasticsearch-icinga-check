use Time::HiRes qw(gettimeofday);
use Time::Piece;
use DateTime::Format::Strptime;

sub buildMetricsIndices {
    my $now = localtime;
    $year = $now->year;
    $month = $now->mon;
    $day = $now->mday;
    my $pattern = "%Y/%m/%d";
    my $parser = DateTime::Format::Strptime->new(
        pattern => $pattern,
        on_error => 'croak',
    );
    my $date = "$year/$month/$day";
    my $parsedDate = $parser->parse_datetime($date);

    my $indexCount = 1;
    my $index;

    my @indexPatterns = ("metrics-{yyyy}.{mm}.{dd}", "metrics-{yyyy}.{mm}");
    if($indexPattern == "logstash-{yyyy}.{mm}.{dd}") {
        @indexPatterns = ($indexPattern);
    }

    while ($indexCount <= $earliestIndexCount) {
        foreach my $indexPattern (@indexPatterns) {
            my $year = $parsedDate->year;
            my $month = $parsedDate->month;
            if($month < 10){
              $month = "0$month"
            }
            my $day = $parsedDate->day;
            if($day < 10){
              $day = "0$day"
            }
            $indexPattern=~ s/{yyyy}/$year/g;
            $indexPattern =~ s/{mm}/$month/g;
            $indexPattern =~ s/{dd}/$day/g;
            $indexPattern = "$indexPattern,";
            $index = "$index$indexPattern"
        }
        $parsedDate->subtract(days => 1);
        $indexCount++;
    }

    chop($index);
    print($index);
    return $index
}

buildMetricsIndices
