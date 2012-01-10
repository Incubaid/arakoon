================
Arakoon Man Page
================
To know all the commands and options of Arakoon, you can either consult the
man pages or the help function.

::

    # arakoon --help
    usage: arakoon --<command>

    If you're clueless, try arakoon --help

      --node runs a node
      --test-node runs a node
      --list-tests lists all possible tests
      --run-all-tests runs all tests
      --run-all-tests-xml <filename> : runs all tests with XML output to file
      --run-some-tests run tests matching filter
      --truncate-tlog <filename> : truncate a tlog after the last valid entry
      --dump-tlog <filename> : dump a tlog file in readable format
      --make-tlog <filename> <counter> : make a tlog file with 1 NOP entry @ <counter>
      --dump-store <filename> : dump a store
      --compress-tlog compress a tlog file
      --uncompress-tlog uncompress a tlog file
      --set <key> <value> : arakoon[<key>] = <value>
      --get <key> : arakoon[<key>]
      --delete <key> : delete arakoon[<key>]
      --benchmark run a benchmark on an existing Arakoon cluster
      --who-master tells you who's the master
      --expect-progress-possible tells you if the master thinks progress is possible
      --statistics returns some master statistics
      --run-system-tests run system tests (you need a running installation
      --version shows version
      -config specifies config file (default = cfg/arakoon.ini )
      -catchup-only will only do a catchup of the node, without actually starting it (option to --node)
      -daemonize add if you want the process to daemonize (only for --node)
      -value_size size of the values (only for --benchmark)
      -tx_size size of transactions (only for --benchmark)
      --test-repeat <repeat_count>
      --collapse <tlog_dir> <n> collapses n tlogs from <tlog_dir> into head database
      --collapse-remote <cluster_id> <ip> <port> <n> tells node to collapse <n> tlogs into its head database
      -help  Display this list of options
      --help  Display this list of options
