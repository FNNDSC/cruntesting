#!/bin/bash

G_SYNOPSIS="

  NAME

        headnode.bash

  SYNOPSIS

	headnode.bash 	-C <cruntype>			\\
			-q <queue>			\\
			-p <PYTHONPATH>			\\
			-r <scriptDir>			\\
			-s <script>			\\
                        -t <computeNodeScratch>         \\
			-c <children>			\\
			-m <sleepMaxLength>		\\
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

	-C <cruntype>
	The crun type to run.
	
	-q <queue>
	If specified, the queue for the <cruntype> object to use.

	-p <PYTHONPATH>
	The PYTHONPATH in the compute node file system space.

	-r <scriptDir>
	The directory containing the script to run in the compute node file
	system space.

	-s <script>
	The actual script to run on the compute node.

        -t <computeNodeScratch>
        The scratch space accessible/visible to the compute node.

	-c <children>
	The number of children that the compute node will attempt to schedule.

	-m <sleepMaxLength>
	The random sleep length for each child.

	-n
	If passed, do not pass computenode.py a cleanup signal.

	-w
	If passed, then computenode.py will wait on filesystem. Otherwise
	will wait by post-blocking on internal crun.

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
G_CRUNTYPE="crun_hpc_slurm"
G_CRUNQUEUE=""
G_CLEANUPARGS="--cleanup"
G_INTERNALWAIT="--internalWait"
G_CNODESCRATCHPATH="~/scratch"

while getopts C:q:p:r:s:c:m:v:nt:w option ; do
        case "$option"
        in
		C) G_CRUNTYPE=$OPTARG			;;
		q) G_CRUNQUEUE=$OPTARG			;;
                p) G_PYTHONPATH=$OPTARG			;;
		r) G_SCRIPTDIR=$OPTARG			;;
		s) G_SCRIPT=$OPTARG			;;
		c) G_CHILDREN=$OPTARG			;;
		m) G_MAXSLEEPLENGTH=$OPTARG		;;
		n) G_CLEANUPARGS="--no-cleanup"		;;
		w) G_INTERNALWAIT="--no-internalWait"	;;
		t) G_CNODESCRATCHPATH=$OPTARG		;;
		v) let Gi_verbose=$OPTARG       	;;
                \?) echo "$G_SYNOPSIS"
                    exit 0;;
        esac
done

shift $(($OPTIND - 1))
CMD=$*

printf "$(date) $(hostname) | Starting computenode job...\n"
Nice..
export PYTHONPATH=$G_PYTHONPATH
#export PYTHONPATH=/home/rudolph/chris/lib/py
USER=$(whoami)
HEADNODE=$(hostname -s)

printf "$(date) $(hostname) | Running child job through scheduler -- will spawn $G_CHILDREN subjobs...\n"
if [[ $G_INTERNALWAIT == "--internalWait" ]] ; then
	printf "$(date) $(hostname) | child will monitor subjobs through internal crun...\n"
else
	printf "$(date) $(hostname) | child will monitor subjobs by checking filesystem...\n"
fi

#echo "$CMD"

if (( ${#G_CRUNQUEUE} )) ; then
	QUEUEARGS=" -q $G_CRUNQUEUE "
else
	QUEUEARGS=""
fi

RUN="./_common/crun.py           			                \
    --echo						                \
    -u $USER		    				                \
    --host $HEADNODE	    				                \
    -s $G_CRUNTYPE	    				                \
    $QUEUEARGS 						                \
    --blockOnChild	    				                \
    --out $G_CNODESCRATCHPATH/$$.headnode.jobout 	                \
    --err $G_CNODESCRATCHPATH/$$.headnode.joberr 	                \
    -c \"   export PYTHONPATH=$PYTHONPATH;                              \
            export PYTHONWARNINGS=ignore;                               \
            export PATH=/export/home/rpienaar/arch/Linux64/bin:$PATH ;  \
            cd $G_SCRIPTDIR;                                            \
            $G_SCRIPTDIR/$G_SCRIPT                                      \
                --user $USER                                            \
                --headnode $HEADNODE                                    \
                --cnodescratchpath $G_CNODESCRATCHPATH                  \
                --crun $G_CRUNTYPE                                      \
                $QUEUEARGS                                              \
                --out $G_CNODESCRATCHPATH/$$.computenode.jobout         \
                --err $G_CNODESCRATCHPATH/$$.computenode.joberr         \
                --children $G_CHILDREN                                  \
                --sleepMaxLength $G_MAXSLEEPLENGTH                      \
                $G_CLEANUPARGS $G_INTERNALWAIT                          \
                $CMD\""
#                $CMD 2>/dev/null\""
RUN=$(echo "$RUN" | sed 's/  */ /g')
echo "$RUN"
eval "$RUN"

printf "$(date) $(hostname) | Completed computenode job.\n"
