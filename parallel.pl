#!/usr/bin/perl
use strict;
use constant { true => 1, false => 0 };
use constant { not_started => 0, started => 1, finished => 2 };

# TODO:
# ✅ assemble all the input/error/status in the order
# ✅ time all the commands
# ✅ manage pipe in commands
# ✅ time total run
# ✅ manage retry option
# ✅ display results
# ✅ manage failsafe or exitonfail
# ✅ manage quiet mode
# ✅ manage overall return status
# ✅ manage timeout
# ✅ end recap
# ✅ autogenerate man page and usage
# ✅ calculate the number of threads based on number of command by default
# ✅ manage argument as file
# - code cleanup
# - publish on github
use utf8;
use open qw(:std :encoding(UTF-8));
use Getopt::Long qw(Configure);
use File::Basename;

# no warnings 'threads';
use threads;
use Thread::Queue;
use Symbol 'gensym'; # vivify a separate handle for STDERR
use threads::shared;

use File::Temp qw/tempdir/;
use File::Slurp;
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;

use Test::More;

my $T0;
my @cmd_execution;
my $status_tag;
my $tmp_dir = tempdir(CLEANUP => 1);

my $cmd = basename($0);
my %DEFAULT = (threads => 32, retry => 0);

$SIG{INT} = $SIG{TERM} = sub {
    # ctrl C and graceful kill
    print "user interruption detected\n";
    $_->kill('INT') for threads->list();
    sleep 1;
    $_->detach() for threads->list(threads::running);

    $status_tag = " (interrupted)";
    exit final_status();
};

my %P = %DEFAULT;
Configure("pass_through");
my $arg = GetOptions(
    "threads=i", \$P{threads},
    "retry=i", \$P{retry},
    "abort-on-failure", \$P{exitonfail},
    "max-time=i", \$P{maxtime},
    "quiet", \$P{quiet},
    "no-summary", \$P{nosummary},
    "unit-test", \$P{test},
    "help", \$P{help}
);

usage() if $P{help};
unit_tests() if $P{test};
usage("invalid arguments") unless $arg;
usage("require file containing cmd list or '-' to specify STDIN") unless @ARGV <= 1;

my $input_fh = \*STDIN;
my $input_file = $ARGV[0];
if (defined $input_file && $input_file ne "-") {
    open $input_fh, "<$input_file" or usage("cannot read from $input_file file");
}

END {
    # clean up threads
    $_->detach() for threads->list(threads::running);
    final_report() if $T0;
}

###
### process input
###
my @cmds = <$input_fh>;
chomp @cmds;
@cmds = grep(/\S/, @cmds);
s/^(\s+)|(\s+)$//g for @cmds;

$T0 = [ gettimeofday ]; # overall time measurement kick-off

my $number_of_cmd_to_run = scalar @cmds;
$P{threads} = $P{threads} > $number_of_cmd_to_run ? $number_of_cmd_to_run : $P{threads};

###
### prepare thread pool and communication queues
###
my $feeder_queue = Thread::Queue->new();
my $finish_queue = Thread::Queue->new();

# create worker pool
my @worker_pool;
for (1 .. $P{threads}) {
    my $thr = threads->create(
        # thread routine
        sub {
            my ($thread_index) = @_;
            $SIG{'INT'} = sub {
                threads->detach();
                threads->exit()
            };

            while (defined(my $cmd_ref = $feeder_queue->dequeue())) {

                my $t0;
                $t0 = [ gettimeofday ]; # start timer
                $cmd_ref->{execution} = started;

                my $tmp_output = "$tmp_dir/$thread_index.out";
                my $tmp_err = "$tmp_dir/$thread_index.err";

                my $retry = $P{retry};
                my $status;
                do {
                    system("($cmd_ref->{cmd}) > $tmp_output 2> $tmp_err");
                    $status = $? >> 8;
                } while ($retry-- && $status);

                my $attempt = $P{retry} - $retry;

                my $stdout = read_file($tmp_output);
                my $stderr = read_file($tmp_err);

                $cmd_ref->{threadindex} = $thread_index;
                $cmd_ref->{status} = $status;

                $cmd_ref->{attempt} = $attempt;
                $cmd_ref->{out} = $stdout;
                $cmd_ref->{err} = $stderr;

                my $duration = int tv_interval($t0); # stop timer
                $cmd_ref->{duration} = $duration;

                $cmd_ref->{execution} = finished;
                $finish_queue->enqueue($cmd_ref);
            }
            return;
        },
        # arguments
        $_
    );
    push @worker_pool, $thr;
}

# create final receiver
my $final_receiver = threads->create(
    # thread routine
    sub {
        my ($expected) = @_;
        $SIG{'INT'} = sub {
            thread->detach();
            threads->exit()
        };

        # my @received_report;
        while (defined(my $cmd_ref = $finish_queue->dequeue())) {
            # push @received_report, $cmd_ref;

            return if $P{exitonfail} && $cmd_ref->{status};
            last unless --$expected;
        }

        return;
    },
    # arguments
    $number_of_cmd_to_run
);

###
### feed the queue with command to run in parallel
###
my $cmd_index = 1;
for (@cmds) {
    my $cmd_dashboard :shared = shared_clone({
        cmdindex  => $cmd_index++,
        cmd       => $_,
        execution => not_started
    });

    push @cmd_execution, $cmd_dashboard;
    $feeder_queue->enqueue($cmd_dashboard);
}

###
### wait till receiver thread is finished
###
while (!$final_receiver->is_joinable()) {
    sleep 0.05;
    if (defined $P{maxtime} && int tv_interval($T0) > $P{maxtime}) {
        $status_tag = " (timed out)";
        exit final_status();
    }
}
$final_receiver->join();

# clean up threads
$_->detach() for threads->list(threads::running);

exit final_status();

sub final_status {
    my $status = 0;
    for (@cmd_execution) {
        $status |= !defined $_->{status} || $_->{status};
    }
    return $status;
}

sub final_report {
    unless ($P{quiet}) {
        for (sort {$a->{cmdindex} <=> $b->{cmdindex}} @cmd_execution) {
            my $attempt_str;

            my $status_str = $_->{status} == 0 ? "success" : "failure";
            if ($_->{execution} == finished) {
                $attempt_str = $_->{attempt} > 1 ? " ($status_str after $_->{attempt} attempts)" : "";
            }
            else {
                $attempt_str = $_->{execution} == not_started ? "not started" : "aborted";
            }
            $_->{display_time} = time_for_human_reader($_->{duration});

            my ($out_count, $out_lines, $out) = process_output($_->{out});
            my ($err_count, $err_lines, $err) = process_output($_->{err});

            my $line_statement = $out_count || $err_count ?
                "STDOUT $out_lines, STDERR $err_lines" :
                "no output";

            my $output = <<EOM;
$_->{cmd} @{ [ visual_status($_->{status}) ]}
  Status : $_->{status}$attempt_str
  Duration : $_->{display_time}
  Output stats: $line_statement
EOM
            $output =~ s|^|/// |gm;
            my ($sep_top, $sep_bottom) =
                build_section_separators("/", 80, 3, " Command # $_->{cmdindex} ");
            print "$sep_top\n$output$sep_bottom\n";

            if ($out =~ /\S/) {
                print "/// STDOUT ($_->{cmd})->\n$out";
            }
            if ($err =~ /\S/) {
                print "/// STDERR ($_->{cmd})->\n$err";
            }
        }
    }

    unless ($P{nosummary}) {
        # recap
        my $total_duration = int tv_interval($T0);
        my ($sep_top, $sep_bottom) = build_section_separators("#", 80, 3, " SUMMARY ");

        print "\n";
        print "$sep_top\n";
        print "TOTAL EXECUTION TIME: " . time_for_human_reader($total_duration) . "\n";

        for (sort {$a->{cmdindex} <=> $b->{cmdindex}} @cmd_execution) {
            my $display_time = " [$_->{display_time}]" if $_->{display_time};
            print "  " . visual_status($_->{status}) . " $_->{cmd}$display_time\n";
        }
        print "\n";
        print "Overall Status: " . visual_status(final_status()) . "$status_tag\n";
        print "$sep_bottom\n";
    }
}

sub time_for_human_reader {
    my ($amount_of_seconds) = @_;
    return "" unless defined $amount_of_seconds && $amount_of_seconds =~ /^\d+$/;

    my $days = int($amount_of_seconds / 86400);
    $amount_of_seconds -= ($days * 86400);
    my $hours = int($amount_of_seconds / 3600);
    $amount_of_seconds -= ($hours * 3600);
    my $minutes = int($amount_of_seconds / 60);
    my $seconds = $amount_of_seconds % 60;

    $days = $days < 1 ? '' : $days . 'd ';
    $hours = $hours < 1 && !$days ? '' : $hours . 'h ';
    $minutes = $minutes < 1 && !$hours ? '' : $minutes . 'm ';

    return $days . $hours . $minutes . $seconds . 's';
}

sub process_output {
    my ($str) = @_;

    return (0, "0 line", $str) if !defined $str || $str eq "";
    chomp $str;
    $str .= "\n";

    my $line_count = (scalar split(/\n/, $str, -1)) - 1;
    my $line_statement = "$line_count line" . ($line_count > 1 ? "s" : "");
    return ($line_count, $line_statement, $str);
}

sub build_section_separators {
    my ($pattern, $length, $start_replacement, $replacement) = @_;

    my $sep_top = substr($pattern x $length, 0, $length);
    my $sep_bottom = $sep_top;
    if (defined $replacement) {
        my $replacement_length = length $replacement;
        $sep_top =~ s/^(.{$start_replacement}).{0, $replacement_length}/$1$replacement/;
        $sep_top = substr($sep_top, 0, $length);
    }
    return ($sep_top, $sep_bottom);
}

sub visual_status {
    my ($status) = @_;
    return "⚠️" unless defined $status;
    return $status ? "❌" : "✅";
}

sub usage {
    my ($error_msg) = @_;

    print "\nERROR: $error_msg\n\n" if defined $error_msg;

    use constant { man => 1, synopsys => 2 };
    my $mode = defined $error_msg ? synopsys : man;

    my $pod_filename = "$tmp_dir/pod";
    open PODFILE, "> $pod_filename" or die $!;

    my $pod = <<EOP;
<=>encoding UTF-8
<=>head1 NAME
$cmd - run multiple system commands in parallel
EOP

    $pod .= <<EOP;
<=>head1 SYNOPSYS
<=>over 4
<=>item $cmd [--help] [--threads I<num>] [--retry I<num>] [--abort-on-failure]
[--max-time I<seconds>] [--unit-test] [--quiet] [--no-summary] [B<I<file>>]
<=>back
<=>head2 EXAMPLE

  echo "sleep 2 \\n ls -al \\n sleep 10" | $cmd -

Output: (partial)

  ### SUMMARY ####################################################################
  TOTAL EXECUTION TIME: 10s
    ✅ sleep 2 [2s]
    ✅ ls -al [0s]
    ✅ sleep 10 [10s]

  Overall Status: ✅
  ################################################################################

EOP

    $pod .= <<EOP if $mode == man;
<=>head1 DESCRIPTION

$cmd expect to receive the list of several shell commands.
The different commands are separated by carriage return (\\n) and can be extracted either from a given I<file> or taken directly from the standard input (when no I<file> is specified, or when it
is equal to '-').

The following options are available:
<=>over 4

<=>item --abort-on-failure
This flag indicates to stop immediately the overall processing when any of the commands fails.
The definition of command failure is when the exit status of the command is different than 0.
The retry mechanism, when set will prevail before deciding about the command status and
possible abortion.

<=>item --help
Print this help message.

<=>item --max-time I<num>
I<num> sets the maximum time the overall processing may last. This timeout mechanism guarantees
the script will end at some point. There is no default value for this option, meaning
there is no limit of time for the script to run.

<=>item --no-summary
limits the display by eliminating the final recap section.
This setting does not affect the execution output for the different commands (see --quiet).

<=>item --quiet
affects the display of the different command executions. Only the summary will be printed
(assuming the --no-summary option is also not set).

Using both --quiet and --no-summary will
totally silent the output. This can be a good choice in very rare cases when only the
script output status matters.

<=>item --retry I<num>
defines the maximum number of attempt a failing command will be retried. By default,
no retry is performed, meaning the status of the first run is final. When --retry is set,
it allows to retry I<num> times after the first failure or less in case of
command success.

<=>item --threads I<num>
I<num> defines the prepared threads of the pool to run commands in parallel.
When not specified the number of prepared threads will correspond to the exact number of given commands to run,
but up to the maximum of $DEFAULT{threads}.

What is valid reason to downsized it (less threads than commands):
It can be a good option for commands that are resource heavy. This would allow to control the maximum
of command that can be run in parallel.

The maximum limit ($DEFAULT{threads}) can be overpassed by setting the value to a higher number,
but be vigilant and keep in mind creating threads are also expensive and can result
to higher cost or even resource failure.

When it comes to parallelize a very large number of commands, it is generally good
to test bench with different values in the right environment context to get a sense about what is "optimal" for your case.
Context switching, high memory, processing power, there are plenty of factors to consider here.

<=>item --unit-test
this option triggers the script unit tests.
<=>back

EOP
    $pod .= <<EOP if $mode == man;
<=>head1 EXIT STATUS
<=>head1 MORE EXAMPLES
<=>head1
<=>head2 Simple run of multiple commands
<=>head2

Command:

  for i in {1..8}; do echo "sleep \$i"; done; | parallel.pl

Output: (truncated)

  ### SUMMARY ####################################################################
  TOTAL EXECUTION TIME: 8s
    ✅ sleep 1 [1s]
    ✅ sleep 2 [2s]
    ✅ sleep 3 [3s]
    ✅ sleep 4 [4s]
    ✅ sleep 5 [5s]
    ✅ sleep 6 [6s]
    ✅ sleep 7 [7s]
    ✅ sleep 8 [8s]

  Overall Status: ✅
  ################################################################################

=> results show the parallel effect, all the commands ran in 8s (time of the slowest one)

<=>head2 Run with a reduced number of threads
<=>head2

Command:

  (for i in {1..8}; do echo "sleep \$i"; done;) | parallel.pl --threads 3

Output: (truncated)

  ### SUMMARY ####################################################################
  TOTAL EXECUTION TIME: 15s
    ✅ sleep 1 [1s]
    ✅ sleep 2 [2s]
    ✅ sleep 3 [3s]
    ✅ sleep 4 [4s]
    ✅ sleep 5 [5s]
    ✅ sleep 6 [6s]
    ✅ sleep 7 [7s]
    ✅ sleep 8 [8s]

  Overall Status: ✅
  ################################################################################

=> here there are more commands to run than threads ready in the pool,
eg. some commands will have to wait for their turn before starting.
This limitation has an effect on the overall timing which is greater than the slowest individual timing.
However it is still beneficial comparing to run all the commands sequentially.

<=>head2 Running commands that can be pre-interpreted by the shell at launch time
(generally not the expected behavior). Don't forget to backslash the different elements to defer
the interpretation at the execution time.
<=>head2

Command:

  cat<<END | parallel.pl
  ls -al
  for f in \\\`ls\\\`; do wc -l \\\$f; done
  for run in {1..3}; do sleep 1; echo \\\$run; done
  END

Output: (truncated)

  /// Command # 3 ////////////////////////////////////////////////////////////////
  ///   for run in {1..3}; do sleep 1; echo \$run; done ✅
  ///   Status : 0
  ///   Duration : 3s
  ///   Output stats: STDOUT 3 lines, STDERR 0 line
  ////////////////////////////////////////////////////////////////////////////////
  /// STDOUT (for run in {1..3}; do sleep 1; echo \$run; done)->
  1
  2
  3

  ### SUMMARY ####################################################################
  TOTAL EXECUTION TIME: 3s
    ✅ ls -al [0s]
    ✅ for f in `ls`; do wc -l \$f; done [0s]
    ✅ for run in {1..3}; do sleep 1; echo \$run; done [3s]

  Overall Status: ✅
  ################################################################################


<=>head2 Run from a perl script using system():
<=>head2

  my \$cmds_as_string = join("\\n", \@cmds);
  system(<<EOM);
  cat<<END | parallel.pl
  \$cmds_as_string
  END
  EOM

<=>head1 USE CASES
<=>head1
This script is designed to run multiple system commands in parallel to optimize
the time and take advantage of computing resources.

This can become very handy to manage in automation like setting up of
virtual environments, execute multiple compilations in parallel, building and publishing multiple artifacts, ...
In short, this can be very handy in CI context, when defining custom scripts.

However there are potential for many other use cases like for instance the processing of
large amount of data (eg. collection, filtering, transformation, aggregation) using divide and conquer
or map/reduce technics. It is just a matter of creativity.

<=>head1 AUTHOR
This program was created by Christophe (Xtof) Dehaudt in dec 2024.

EOP

    # use a fake marker <=> to prevent autogenerated pod to be detected
    # by perldoc run on source file
    # also make sure to separate section tag with proper spacing
    $pod =~ s/^<=>(.*)$/\n=$1\n/gm;

    print PODFILE $pod;
    close PODFILE;

    my $cmd = "pod2text -t $pod_filename";
    $cmd .= " | less -R" if $mode == man;
    system($cmd);

    exit($error_msg ? 1 : 0);
}


###
### TESTS
###
sub unit_tests {

    my $display_argument = sub {
        # transform args into a string
        my ($argument) = @_;

        $argument = [ $argument ] if ref($argument) ne 'ARRAY';

        my $display_model = sub {
            my ($str) = @_;
            return "undef" unless defined $str;
            return $str if /(^[+-]?\d+$)/; # a number
            return "\"$_\"";
        };
        my $arg_as_str = join(', ', map {$display_model->($_)} @$argument);

        $arg_as_str =~ s/\n/\\n/g;
        return $arg_as_str;
    };

    my $test_time_for_human_reader = sub {
        my ($argument, $expected) = @_;
        my $arg_str = $display_argument->($argument);
        is(time_for_human_reader($argument), $expected, "time_for_human_reader($arg_str)");
    };
    $test_time_for_human_reader->(1, "1s");
    $test_time_for_human_reader->(59, "59s");
    $test_time_for_human_reader->(60, "1m 0s");
    $test_time_for_human_reader->(85, "1m 25s");
    $test_time_for_human_reader->(3600, "1h 0m 0s");
    $test_time_for_human_reader->(10244, "2h 50m 44s");
    $test_time_for_human_reader->(86400, "1d 0h 0m 0s");
    $test_time_for_human_reader->(34567890, "400d 2h 11m 30s");
    $test_time_for_human_reader->(undef, "");
    $test_time_for_human_reader->(-1, "");

    my $test_process_output = sub {
        my ($argument, $expected) = @_;
        my $arg_str = $display_argument->($argument);
        is_deeply([ process_output($argument) ], $expected, "process_output($arg_str)");
    };

    $test_process_output->("abc", [ 1, "1 line", "abc\n" ]);
    $test_process_output->("abc\n", [ 1, "1 line", "abc\n" ]);
    $test_process_output->("abc\n123", [ 2, "2 lines", "abc\n123\n" ]);
    $test_process_output->("abc\n123\n", [ 2, "2 lines", "abc\n123\n" ]);
    $test_process_output->("abc\n\n123\n\n", [ 4, "4 lines", "abc\n\n123\n\n" ]);
    $test_process_output->("", [ 0, "0 line", "" ]);
    $test_process_output->(" ", [ 1, "1 line", " \n" ]);
    $test_process_output->("\n", [ 1, "1 line", "\n" ]);

    my $test_build_section_separators = sub {
        my ($argument, $expected) = @_;
        my $arg_str = $display_argument->($argument);
        is_deeply([ build_section_separators(@$argument) ], $expected, "build_section_separators($arg_str)");
    };

    $test_build_section_separators->([ "x", 5 ], [ "xxxxx", "xxxxx" ]);
    $test_build_section_separators->([ "xx", 5 ], [ "xxxxx", "xxxxx" ]);
    $test_build_section_separators->([ "z", 6, 3 ], [ "zzzzzz", "zzzzzz" ]);
    $test_build_section_separators->([ "z", 6, 3, "Z" ], [ "zzzZzz", "zzzzzz" ]);
    $test_build_section_separators->([ "z", 6, 3, " Z " ], [ "zzz Z ", "zzzzzz" ]);
    $test_build_section_separators->([ "z", 6, 2, " Z " ], [ "zz Z z", "zzzzzz" ]);
    $test_build_section_separators->([ "y", 6, 4, "WXYZ" ], [ "yyyyWX", "yyyyyy" ]);
    $test_build_section_separators->([ "y", 6, 5, "WXYZ" ], [ "yyyyyW", "yyyyyy" ]);
    $test_build_section_separators->([ "y", 6, 6, "WXYZ" ], [ "yyyyyy", "yyyyyy" ]);
    $test_build_section_separators->([ "y", 6, 100, "WXYZ" ], [ "yyyyyy", "yyyyyy" ]);
    done_testing();
    exit;
}

__END__


=head1 coucou