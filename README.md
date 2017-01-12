# elasticsearch-icinga-check
Perl script to check metrics from elasticsearch for use as a custom Icinga2 check modelled after [this graphite check](https://github.com/disqus/nagios-plugins)

## Usage
```
check-elasticsearch-metrics.pl [OPTIONS]

Required Settings:
        -c [threshold]: critical threshold
        -w [threshold]: warning threshold
        -s [seconds]: number of seconds from now to check
        -q [query_string]: the query to run in elasticsearch
        -h [host]: elasticsearch host
        -i [number_of_indices]: the number of indices to go back through, defaults to 2
        -n [index_pattern]: the pattern expects months and years and can take aprefix and days, e.g: metrics-{yyyy}.{mm}

        Optional Settings:
        -?: this help message
        -r: reverse threshold (so amounts below threshold values will alert)
        -p [port]: elasticsearch port (defaults to 9200)
        -a [name]: aggregation name
        -t [type]: aggregation type
        -f [field_name]: the name of the field to aggregate
        -x [indices_prefix]: the prefix of your elasticsearch indices
        -d: include the day in elasticsearch index
        -z: debug mode

Error codes:
        0: Everything OK, check passed
        1: Warning threshold breached
        2: Critical threshold breached
        3: Unknown, encountered an error querying elasticsearch

```

## Icinga2 Config

In order to use this check in a service or host, you'll need something like the following set up in your icinga custom_commands.conf:

```
object CheckCommand "check-elasticsearch" {
  import "plugin-check-command"
  command = [ "/path/to/check-elasticsearch-metrics.pl" ]
  arguments = {
    "-c" = "$elasticsearch_critical$"
    "-w" = "$elasticsearch_warning$"
    "-q" = "$elasticsearch_query$"
    "-a" = "$elasticsearch_aggregation_name$"
    "-f" = "$elasticsearch_aggregation_field$"
    "-t" = "$elasticsearch_aggregation_type$"
    "-s" = "$elasticsearch_seconds$"
    "-x" = "$elasticsearch_index_prefix$"
    "-h" = "elasticsearch.host.example.com"
    "-n" = "{prefix}-{yyyy.{mm}"
    "-p" = "9200"
    "-r" = {
        set_if = "$elasticsearch_reverse$"
        description = "Reverse - Alert when the value is UNDER warn/crit instead of OVER"
    }
  }
}
```

With that set up as a custom command you can use it in a service like this:

```
Service "Errors ES" {
	name = "Errors ES"
	check_command = "check-elasticsearch"
	check_interval = "60"
	retry_interval = "180"
	vars.elasticsearch_critical = "10"
	vars.elasticsearch_warning = "50"
	vars.elasticsearch_query = "metric:web_error"
	vars.elasticsearch_aggregation_name = "numberOfErrors"
	vars.elasticsearch_aggregation_field = "value"
	vars.elasticsearch_aggregation_type = "sum"
	vars.elasticsearch_seconds = 60 * 60
    vars.elasticsearch_index_prefix = "metrics"
}
```

Which will alert when it detects more than 10 errors over the space of an hour.

# Summary

That's about it, I'm accepting pull requests so feel free to fork!
