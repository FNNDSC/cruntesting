#!/usr/bin/env python
#
# NAME
#
#        computenode.py
#
# DESCRIPTION
#
#       'computenode.py' uses a crun object to ssh back to a target
#       headnode and schedule several compute/process tasks.
#
# HISTORY
#   07 November 2014
#   o Initial design and coding.
#


# System imports
import  os
import  sys
import  argparse
import  tempfile, shutil
import  time
import  random
from    _common import systemMisc       as misc
from    _common import crun


class childScheduler:
    '''
    This class schedules a given processing job on a
    targetted node.

    '''

    #
    # Class member variables -- if declared here are shared
    # across all instances of this class
    #
    _dictErr = {
        'Load'              : {
            'action'        : 'attempting to pickle load object, ',
            'error'         : 'a PickleError occured.',
            'exitCode'      : 14},
        'outDirNotCreate': {
            'action'        : 'attempting to create the <outDir>, ',
            'error'         : 'a system error was encountered. Do you have create permission?',
            'exitCode'      : 13},
    }

    def __init__(self, **kwargs):
        '''
        Basic constructor. Checks on named input args, checks that files
        exist and creates directories.

        '''

        self._lw                        = 120
        self._rw                        = 20

        self._topDir                    = ''

        self._l_cmd                     = []
        self._b_sleepCmd                = False
        self._sleepMaxLength            = 0
        self._numberOfChildren          = 1

        self._str_remotePath            = '~/chris/src/cruntesting'
        self._str_remoteCrun            = 'crun_hpc_slurm'
	self._str_schedulerStdOutDir	= 'jobOutDir'
	self._str_schedulerStdErrDir	= 'jobErrDir'
	self._b_cleanup			= True
	self._b_internalWait		= True
	self._pythonpath		= ''

        # A local "shell"
        self.OSshell = crun.crun()
        self.OSshell.echo(False)
        self.OSshell.echoStdOut(False)
        self.OSshell.detach(False)
        self.OSshell.waitForChild(True)

	self.OSshell('whoami')
	self._str_remoteUser	= self.OSshell.stdout()
	self.OSshell('hostname -s')
	self._str_remoteHost	= self.OSshell.stdout()

        for key, value in kwargs.iteritems():
            if key == 'crun':           	self._str_remoteCrun		= value
	    if key == 'headnode':		self._str_remoteHost		= value
	    if key == 'user':			self._str_remoteUser		= value
	    if key == 'cnodescratchpath':	self._str_cnodescratchpath	= value
            if key == 'jobOutDir':         	self._str_schedulerStdOutDir	= value
            if key == 'jobErrDir':         	self._str_schedulerStdErrDir	= value
            if key == 'cmd':            	self._l_cmd             	= value
            if key == 'children':       	self._numberOfChildren  	= int(value)
            if key == 'sleepMaxLength': 	self._sleepMaxLength    	= int(value)
            if key == 'b_cleanup': 		self._b_cleanup			= value
            if key == 'pythonpath': 		sys.path.append(value)
	    if key == 'b_internalWait':		self._b_internalWait		= int(value)

        # The remote/scheduler shell
	print('remoteUser 	        = %s' % self._str_remoteUser)
	print('remoteHost 	        = %s' % self._str_remoteHost)
	print('schedulerStdOutDir	= %s' % self._str_schedulerStdOutDir)
	print('schedulerStdErrDir	= %s' % self._str_schedulerStdErrDir)
        print('crun.' + self._str_remoteCrun + '(remoteUser=self._str_remoteUser,remoteHost=self._str_remoteHost,schedulerStdOutDir=self._str_schedulerStdOutDir,schedulerStdErrDir=self._str_schedulerStdErrDir)')
	print("in computenode.py stdoutDir = %s stderrDir =%s" % (self._str_schedulerStdOutDir, self._str_schedulerStdErrDir))
        self.sshCluster = eval('crun.' + self._str_remoteCrun + '(remoteUser=self._str_remoteUser,remoteHost=self._str_remoteHost,schedulerStdOutDir=self._str_schedulerStdOutDir,schedulerStdErrDir=self._str_schedulerStdErrDir)')

        self.initialize()

    def initialize(self):
        '''
        This method provides some "post-constructor" initialization. It is
        typically called after the constructor and after other class flags
        have been set (or reset).

        '''
        print('number of children = %d' % self._numberOfChildren)
        print('command to execute = ')
        print(self._l_cmd)
        self.OSshell('rm -f *done*')

    def run(self):
        '''
        The 'run' method actually does the work of this class.
        '''

        str_endJobCodon = 'echo #child# > %s/#child#-done.txt' % self._str_cnodescratchpath

	str_cwd		= os.getcwd()

        for c in range(0, self._numberOfChildren):
            print('Spawning child %d.' % c),
            str_cmdEnd = str_endJobCodon.replace('#child#', str(c))
	    str_coreCmd	= " ".join(self._l_cmd)
            # If b_sleepCmd is True, override cmd to a random sleep
            if self._sleepMaxLength:
                sleepLength = random.uniform(0, self._sleepMaxLength)
                str_coreCmd = '%s\n/bin/sleep %d\n' % (\
                                    str_coreCmd,
                                    sleepLength
                                    )
            str_wholeCmd = '#!/bin/bash\n\n\n%s%s' % (str_coreCmd, str_cmdEnd)
	    str_scriptName = "%s/job-%d.crun" % (self._str_cnodescratchpath, c)
	    self.OSshell("echo \"%s\" > %s; chmod 755 %s" % (str_wholeCmd, str_scriptName, str_scriptName))
            print('Child %d will execute: <%s>' % (c, str_scriptName))
	    self.sshCluster.echo(True)
	    self.sshCluster.echoStdOut(True)
            #self.sshCluster(str_wholeCmd)
            self.sshCluster.schedulerStdOutDir(self._str_schedulerStdOutDir)
            self.sshCluster.schedulerStdErrDir(self._str_schedulerStdErrDir)
            self.sshCluster("%s" % (str_scriptName))


        # Now wait for all children to complete
        self.OSshell.echoStdOut(False)
        print('\n\n')
	if self._b_internalWait:
	    self.sshCluster.blockOnChild()
	else:
            while(True):
            	self.OSshell('ls -1 %s/*done* | wc -l' % self._str_cnodescratchpath)
            	numberComplete = int(self.OSshell.stdout())
	    	str_msg = "\t%s children have completed...    \r" % numberComplete
            	print(str_msg),
	    	sys.stdout.flush()
            	if numberComplete == self._numberOfChildren:
                	break
            	time.sleep(1)
	print('\n')
	if self._b_cleanup:
	    print('Cleaning up...')
	    self.OSshell('rm -f %s/*done*' % self._str_cnodescratchpath)
	    self.OSshell('rm -f %s/job-*crun' % self._str_cnodescratchpath)


def synopsis(ab_shortOnly = False):
    scriptName = os.path.basename(sys.argv[0])
    shortSynopsis =  '''
    SYNOPSIS

            %s                                              \\
                            [--children <numberOfChildren>]         \\
                            [--sleepMaxLength <interval>]           \\
                            [--crun <crun_hpc_type>]                \\
                            [--headnode <headnode>]                 \\
                            [--user <user>]                         \\
                            [--cnodescratchpath <dir>]              \\
                            [--out <clusterOutFile>]                \\
                            [--err <clusterErrFile>]                \\
			    [--cleanup | --no-cleanup]		    \\
                            [--internalWait | --no-internalWait]    \\
                            [--pythonpath <dir>]                    \\
                            "Command and args to execute"


    ''' % scriptName

    description =  '''
    DESCRIPTION

        `%s' attempts to schedule a job by creating an appropriate
        crun object. Multiple instances of this object are created
        and "executed". This script will block until all the
        scheduled sub-children are complete (by blocking on a simple
        output counter).

    ARGS

       --children <numberOfChildren>
       The number of children to spawn. All are identical and dispatched
       to the scheduler via a crun object.

       --ctype <crun_hpc_type>
       The crun hpc class to use.

       --sleepMaxLength <interval>
       If passed, then command string will be suffixed with a random sleep
       interval.

       --headnode <headnode>
       The name of the cluster headnode, from the perspective of the computenode.
       Note that in some cases the external public name of the headnode is NOT the
       same as the internal name.

       --user <user>
       The username on the headnode. Most likely this will be the same username
       as that which is associated with the computenode script.

       --cnodescratchpath <dir>
       The directory from the perspective of the computenode to use as scratch space. In
       some cases the computenodes do NOT have access to the normal user space, but
       have their own space.

       --out <clusterOutFile>
       The name of the output file to contain stdout data from the scheduled job (if
       applicable for the given cluster type).

       --err <clusterErrFile>
       The name of the output file to contain stderr data from the scheduled job (if
       applicable for the given cluster type).

       --cleanup | --no-cleanup
       If specified, either remove all the temporary output and job scripts,
       or leave them be.

       --internalWait | --no-internalWait
       If --internalWait, the crun object will block and wait for all scheduled children
       to finish based on query/poll of the scheduler table. If --no-internalWait, the
       script will parse files in the <cnodescratchpath> directory to determine if all
       children are complete. This approach is the legacy approach and not recommended.

       --pythonpath <dir>
       Explicitly add <dir> to the pythonpath.

       "<Command and args to execute>"
       The command string to schedule.

    EXAMPLES


    ''' % (scriptName)
    if ab_shortOnly:
        return shortSynopsis
    else:
        return shortSynopsis + description


#
# entry point
#
if __name__ == "__main__":


    # always show the help if no arguments were specified
    if len( sys.argv ) == 1:
        print synopsis()
        sys.exit( 1 )

    verbosity   = 0

    parser = argparse.ArgumentParser(description = synopsis(True))

    parser.add_argument('l_cmd',
                        metavar='CMD', nargs='+',
                        help='command string')
    parser.add_argument('--verbosity', '-v',
                        dest='verbosity',
                        action='store',
                        default=0,
                        help='verbosity level')
    parser.add_argument('--children', '-c',
                        dest='numberOfChildren',
                        action='store',
                        default='1',
                        help='number of child processes to schedule')
    parser.add_argument('--crun', '-C',
                        dest='crun',
                        action='store',
                        default='crun_hpc_slurm',
                        help='crun object to schedule on cluster')
    parser.add_argument('--headnode', '-H',
                        dest='headnode',
                        action='store',
                        default='',
                        help='name of the headnode')
    parser.add_argument('--err', '-e',
                        dest='err',
                        action='store',
                        default='',
                        help='job stderr')
    parser.add_argument('--out', '-o',
                        dest='out',
                        action='store',
                        default='',
                        help='job stdout')
    parser.add_argument('--user', '-u',
                        dest='user',
                        action='store',
                        default='',
                        help='username on the headnode')
    parser.add_argument('--sleepMaxLength', '-s',
                        dest='sleepMaxLength',
                        action='store',
                        default='0',
                        help='suffix a random length sleep')
    parser.add_argument('--cnodescratchpath', '-S',
                        dest='cnodescratchpath',
                        action='store',
                        default='',
                        help='a path that is accessible to the cnode process')
    parser.add_argument('--pythonpath', '-p',
                        dest='pythonpath',
                        action='store',
                        default='',
                        help='explicitly append to the python path for this script')
    parser.add_argument("--cleanup", help="cleanup temp files", dest='cleanup', action='store_true', default=True)
    parser.add_argument("--no-cleanup", help="don't cleanup temp files", dest='cleanup', action='store_false')
    parser.add_argument("--internalWait", help="wait using scheduler poll", dest='internalWait', action='store_true', default=True)
    parser.add_argument("--no-internalWait", help="wait by checking FS artifacts", dest='internalWait', action='store_false')
    args = parser.parse_args()

    child = childScheduler(
			crun		= args.crun,
			jobOutDir       = args.out,
			jobErrDir	= args.err,
			headnode	= args.headnode,
			user		= args.user,
                        children        = args.numberOfChildren,
                        sleepMaxLength  = args.sleepMaxLength,
                        cmd             = args.l_cmd,
			cnodescratchpath= args.cnodescratchpath,
			pythonpath	= args.pythonpath,
			b_cleanup	= args.cleanup,
			b_internalWait	= args.internalWait
    )
    child.run()
