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
        self._str_remoteHost            = 'mit.eofe4.edu'
        self._str_remoteUser            = 'rudolph'
        self._str_remoteCrun            = 'crun_hpc_slurm'

        # A local "shell"
        self.OSshell = crun.crun()
        self.OSshell.echo(False)
        self.OSshell.echoStdOut(False)
        self.OSshell.detach(False)
        self.OSshell.waitForChild(True)

        # The remote/scheduler shell
        self.sshCluster = crun.crun_hpc_slurm(
                                    remoteUser=self._str_remoteUser,
                                    remoteHost=self._str_remoteHost
                                    )

        for key, value in kwargs.iteritems():
            if key == 'cmd':            self._l_cmd             = value
            if key == 'children':       self._numberOfChildren  = int(value)
            if key == 'sleepMaxLength': self._sleepMaxLength    = int(value)

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

        str_endJobCodon = '; echo #child# > #child#-done.txt'

        str_coreCmd     = self._l_cmd[0]

        for c in range(0, self._numberOfChildren):
            print('Spawning child %d.' % c),
            str_cmdEnd = str_endJobCodon.replace('#child#', str(c))
            # If b_sleepCmd is True, override cmd to a random sleep
            if self._sleepMaxLength:
                sleepLength = random.uniform(0, self._sleepMaxLength)
                self._l_cmd[0] = '%s ; /bin/sleep %d ' % (\
                                    str_coreCmd,
                                    sleepLength
                                    )
            str_wholeCmd = '%s%s' % (self._l_cmd[0], str_cmdEnd)
            print('Child %d will execute: <%s>' % (c, str_wholeCmd))
            self.sshCluster(str_wholeCmd)


        # Now wait for all children to complete
        self.OSshell.echoStdOut(False)
        print('\n\n')
        while(True):
            self.OSshell('ls -1 *done* | wc -l')
            numberComplete = int(self.OSshell.stdout())
            print('%d children have completed...' % numberComplete)
            if numberComplete == self._numberOfChildren:
                break
            time.sleep(1)

def synopsis(ab_shortOnly = False):
    scriptName = os.path.basename(sys.argv[0])
    shortSynopsis =  '''
    SYNOPSIS

            %s                                              \\
                            [--children <numberOfChildren>]         \\
                            [--sleepMaxLength <interval>]           \\
                            --ctype <crun_hpc_type>                 \\
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
    parser.add_argument('--sleepMaxLength', '-s',
                        dest='sleepMaxLength',
                        action='store',
                        default='0',
                        help='suffix a random length sleep')

    args = parser.parse_args()

    child = childScheduler(
                        children        = args.numberOfChildren,
                        sleepMaxLength  = args.sleepMaxLength,
                        cmd             = args.l_cmd
    )
    child.run()
