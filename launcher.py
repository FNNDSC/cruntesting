#!/usr/bin/env python
'''
 NAME

        launcher.py

 DESCRIPTION

       'launcher.py' emulates the behaviour of a web-based launcher.
       This script will ssh to a remote host and execute the bash-based
       'headnode.bash' script on that host.

 HISTORY
   07 November 2014
   o Initial design and coding.
'''

# System imports
import  os
import  sys
import  argparse
import  tempfile, shutil
import  time
import  random
from    _common import systemMisc       as misc
from    _common import crun

class launcherRemote:
    '''
    This class simply logins into a remote host and runs
    a script.

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

        self._str_remotePath            = '~/chris'
        self._str_remoteScript          = 'headnode.bash'
        self._str_remoteHost            = 'mit.eofe4.edu'
        self._str_remoteUser            = 'rudolph'
        self._str_remoteCrun            = 'crun_hpc_mosix'

        for key, value in kwargs.iteritems():
            if key == 'remotePath':     self._str_remotePath    = value
            if key == 'remoteScript':   self._str_remoteScript  = value
            if key == 'remoteHost':     self._str_remoteHost    = value
            if key == 'remoteUser':     self._str_remoteUser    = value
            if key == 'remoteCrun':     self._str_remoteCrun    = value


        # A local "shell"
        self.OSshell = crun.crun()
        self.OSshell.echo(False)
        self.OSshell.echoStdOut(False)
        self.OSshell.detach(False)
        self.OSshell.waitForChild(True)


        # The remote/scheduler shell
        self.sshCluster = crun.crun(remoteUser=self._str_remoteUser,
                                    remoteHost=self._str_remoteHost)
        print("remote call stdout = %s" % self.sshCluster.stdout())
        print("remote call stderr = %s" % self.sshCluster.stderr())


        self.initialize()


    def initialize(self):
        '''
        This method provides some "post-constructor" initialization. It is
        typically called after the constructor and after other class flags
        have been set (or reset).

        '''
        pass

    def run(self):
        '''
        The 'run' method actually does the work of this class.
        '''
        str_cmd = '%s/%s' % (self._str_remotePath, self._str_remoteScript)
        self.sshCluster.echo(True)
        self.sshCluster.echoStdOut(True)

        # fire-and-forget the remote script
        self.sshCluster(str_cmd)



def synopsis(ab_shortOnly = False):
    scriptName = os.path.basename(sys.argv[0])
    shortSynopsis =  '''
    SYNOPSIS

            %s                                         \\
                            [--remoteHost <remoteHost>]         \\
                            [--remoteUser <remoteUser>]         \\
                            [--remotePath <remotePath>]         \\
                            [--remoteScript <remoteScript>]     \\
                            [--remoteCrun <crun_hpc_type>]

    ''' % scriptName

    description =  '''
    DESCRIPTION

        `%s' emulates the ChRIS launcher.php in as much as it
        ssh's to a remote host and executes a bash-based script
        on that host.

    ARGS

       --remoteHost <remoteHost>
       The host to connect to. It is assumed that this is an HPC
       headnode.

       --remoteUser <remoteUser>
       The remote user.

       --remotePath <remotePath>
       The path to the remote script to run on the headnode.

       --remoteScript <remoteScript>
       The remote script to run on the headnode.

       --ctype <crun_hpc_type>
       The crun hpc class to use.

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

    parser.add_argument('--verbosity', '-v',
                        dest='verbosity',
                        action='store',
                        default=0,
                        help='verbosity level')
    parser.add_argument('--remoteHost', '-r',
                        dest='remoteHost',
                        action='store',
                        default='eofe4.mit.edu',
                        help='the remote headnode.')
    parser.add_argument('--remoteUser', '-u',
                        dest='remoteUser',
                        action='store',
                        default='rudolph',
                        help='the headnode user name.')
    parser.add_argument('--remotePath', '-p',
                        dest='remotePath',
                        action='store',
                        default='~/src/devel/distrib',
                        help='the path to the remote script to execute.')
    parser.add_argument('--remoteScript', '-s',
                        dest='remoteScript',
                        action='store',
                        default='headnode.bash',
                        help='the remote script to execute.')
    parser.add_argument('--remoteCrun', '-c',
                        dest='remoteCrun',
                        action='store',
                        default='crun_hpc_mghpcc',
                        help='the remote cluster crun object type.')

    args = parser.parse_args()

    launcher = launcherRemote(
                        remoteHost      = args.remoteHost,
                        remoteUser      = args.remoteUser,
                        remotePath      = args.remotePath,
                        remoteScript    = args.remoteScript,
                        remoteCrun      = args.remoteCrun
    )
    launcher.run()
