#!/bin/bash

G_SYNOPSIS="

  NAME

        headnode.bash

  DESCRIPTION

        'headnode.bash' emulates the ChRIS runtime/scheduling framework
        by acting like a stripped down 'chris.run' script, containing
        only the CLI crun HPC object. This object in turn attempts
        to schedule a child process called 'computenode.py' on a
        compute cluster node.

        It is imperative the at the crun instance created in this
        shell blocks until the scheduled child (and all its children)
        are complete.

  ARGS

  HISTORY

    07-Nov-2014
    o Initial design and coding.

"


printf "Starting computenode job...\n"

./_common/crun.py           \
    -u rudolph              \
    --host eofe4.mit.edu    \
    -s crun_hpc_slurm       \
    --no-setDefaultFlags    \
    --echo --echoStdOut     \
    "bash -c 'computenode.py'"


printf "Completed computenode job.\n"
