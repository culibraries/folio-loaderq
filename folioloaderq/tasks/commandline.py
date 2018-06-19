import StringIO
from subprocess import call,check_output,STDOUT, CalledProcessError


def commandLineExec(command):
    """
    Task run a command line task on the celery worker node.
    args:
        command: type list
    results:
        Output of child process or and Exception for a non Zero return code.
    """
    try:
        return check_output(command)
    except CalledProcessError as err:
        raise Exception("error code: {0}, {1}".format(err.returncode, err.output))
