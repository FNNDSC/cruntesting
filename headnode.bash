#!/bin/bash

G_SYNOPSIS="

  NAME

        headnode.bash

  SYNOPSIS

	headnode.bash 	-p <PYTHONPATH>
			-r <scriptDir>
			-s <script>
			-c <children>
			-m <sleepMaxLength>
			\"command to execute\"
			
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

	-p <PYTHONPATH>
	The PYTHONPATH in the compute node file system space.

	-r <scriptDir>
	The directory containing the script to run in the compute node file
	system space.

	-s <script>
	The actual script to run on the compute node.

	-c <children>
	The number of children that the compute node will attempt to schedule.

	-m <sleepMaxLength>
	The random sleep length for each child.

	\"command to execute\"
	The command that the computenode will schedule.

  HISTORY

    07-Nov-2014
    o Initial design and coding.

"
G_PYTHONPATH="~/chris/lib/py"
G_SCRIPTDIR="~/chris/src/cruntesting"
G_SCRIPT="computenode.py"
G_CHILDREN=10
G_MAXSLEEPLENGTH=20

while getopts p:r:s:c:m:v: option ; do
        case "$option"
        in
                p) G_PYTHONPATH=$OPTARG		;;
		r) G_SCRIPTDIR=$OPTARG		;;
		s) G_SCRIPT=$OPTARG		;;
		c) G_CHILDREN=$OPTARG		;;
		m) G_MAXSLEEPLENGTH=$OPTARG	;;
		v) let Gi_verbose=$OPTARG       ;;
                \?) echo "$G_SYNOPSIS" 
                    exit 0;;
        esac
done

shift $(($OPTIND - 1))
CMD=$*

printf "$(date) $(hostname) | Starting computenode job...\n"

export PYTHONPATH=/home/rudolph/chris/lib/py

./_common/crun.py           \
    -u rudolph              \
    --host eofe4    	    \
    -s crun_hpc_slurm       \
    --no-setDefaultFlags    \
    --echo --echoStdOut     \
    --block		    \
    "bash -c 'export PYTHONPATH=$PYTHONPATH; 
	cd ~/chris/src/cruntesting ; ~/chris/src/cruntesting/computenode.py --children $G_CHILDREN --sleepMaxLength $G_MAXSLEEPLENGTH \"/bin/ls /tmp\"'"


printf "$(date) $(hostname) | Completed computenode job.\n"
