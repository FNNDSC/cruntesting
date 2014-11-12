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
        self._str_remoteHost            = 'eofe4'
        self._str_remoteUser            = 'rudolph'
        self._str_remoteCrun            = 'crun_hpc_slurm'
	self._b_cleanup			= True

        # A local "shell"
        self.OSshell = crun.crun()
        self.OSshell.echo(False)
        self.OSshell.echoStdOut(False)
        self.OSshell.detach(False)
        self.OSshell.waitForChild(True)

        for key, value in kwargs.iteritems():
            if key == 'crun':           self._str_remoteCrun	= value
            if key == 'cmd':            self._l_cmd             = value
            if key == 'children':       self._numberOfChildren  = int(value)
            if key == 'sleepMaxLength': self._sleepMaxLength    = int(value)
            if key == 'b_cleanup': 	self._b_cleanup		= value

        # The remote/scheduler shell
        self.sshCluster = eval('crun.' + self._str_remoteCrun + '(remoteUser=self._str_remoteUser,remoteHost=self._str_remoteHost)')

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

        str_endJobCodon = 'echo #child# > %s/#child#-done.txt' % self._str_remotePath

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
	    str_scriptName = "%s/job-%d.crun" % (self._str_remotePath, c)
	    self.OSshell("echo \"%s\" > %s; chmod 755 %s" % (str_wholeCmd, str_scriptName, str_scriptName))
            print('Child %d will execute: <%s>' % (c, str_scriptName))
	    self.sshCluster.echo(True)
	    self.sshCluster.echoStdOut(True)
            #self.sshCluster(str_wholeCmd)
            self.sshCluster("%s" % (str_scriptName))


        # Now wait for all children to complete
        self.OSshell.echoStdOut(False)
        print('\n\n')
        while(True):
            self.OSshell('ls -1 %s/*done* | wc -l' % self._str_remotePath)
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
	    self.OSshell('rm -f %s/*done*' % self._str_remotePath)
	    self.OSshell('rm -f %s/job-*crun' % self._str_remotePath)
	

def synopsis(ab_shortOnly = False):
    scriptName = os.path.basename(sys.argv[0])
    shortSynopsis =  '''
    SYNOPSIS

            %s                                              \\
                            [--children <numberOfChildren>]         \\
                            [--sleepMaxLength <interval>]           \\
                            [--crun <crun_hpc_type>]                \\
			    [--cleanup | --no-cleanup]		    \\
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

       --cleanup | --no-cleanup
       If specified, either remove all the temporary output and job scripts,
       or leave them be.	

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
    parser.add_argument('--sleepMaxLength', '-s',
                        dest='sleepMaxLength',
                        action='store',
                        default='0',
                        help='suffix a random length sleep')
    parser.add_argument("--cleanup", help="cleanup temp files", dest='cleanup', action='store_true', default=True)
    parser.add_argument("--no-cleanup", help="don't cleanup temp files", dest='cleanup', action='store_false')

    args = parser.parse_args()

    child = childScheduler(
			crun		= args.crun,
                        children        = args.numberOfChildren,
                        sleepMaxLength  = args.sleepMaxLength,
                        cmd             = args.l_cmd,
			b_cleanup	= args.cleanup
    )
    child.run()
