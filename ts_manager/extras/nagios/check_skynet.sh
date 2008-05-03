#! /bin/sh 
#
# Usage: ./check_skynet --help
#
# Examples: 
#	./check_skynet -w 300 -c 2000 -u

# Paths to commands used in this script.  These
# may have to be modified to match your system setup.

PATH=""

ECHO="/bin/echo"
SED="/bin/sed"
GREP="/bin/grep"
TAIL="/bin/tail"
CAT="/bin/cat"
CUT="/bin/cut"
WC="/bin/wc"
CURL="/usr/bin/curl -f"

PROGNAME=`/bin/basename $0`
PROGPATH=`echo $0 | /bin/sed -e 's,[\\/][^\\/][^\\/]*$,,'`
REVISION=`echo '$Revision: 0.1 $' | /bin/sed -e 's/[^0-9.]//g'`

. /usr/local/nagios/libexec/utils.sh

print_usage() {
    echo "Usage: $PROGNAME -w <threshold> -c <threshold> -u <url of skynet status page>"
    echo "Usage: $PROGNAME --help"
    echo "Usage: $PROGNAME --version"
}

print_help() {
    print_revision $PROGNAME $REVISION
    echo ""
    print_usage
    echo ""
    echo "Check Skynet's untaken_tasks"
    echo ""
    support
}

# Make sure the correct number of command line
# arguments have been supplied

if [ $# -lt 3 ]; then
    print_usage
    exit $STATE_UNKNOWN
fi

# Grab the command line arguments

exitstatus=$STATE_UNKNOWN #default
while test -n "$1"; do
    case "$1" in
        --help)
            print_help
            exit $STATE_OK
            ;;
        -h)
            print_help
            exit $STATE_OK
            ;;
        --version)
            print_revision $PROGNAME $VERSION
            exit $STATE_OK
            ;;
        -V)
            print_revision $PROGNAME $VERSION
            exit $STATE_OK
            ;;
        -w)
	    WARNING=$2;
            shift;
            ;;
        -c)
	    CRITICAL=$2;
            shift;
            ;;
        -u)
	    URL=$2;
            shift;
            ;;
        *)
            echo "Unknown argument: $1"
            print_usage
            exit $STATE_UNKNOWN
            ;;
    esac
    shift
done


CURRENT=$($CURL $URL | $GREP "untaken_tasks\|down"| $CUT -d: -f2)

if [ -z $CURRENT ] ;then
	$ECHO "CANNOT GATHER SKYNET TASKS CALL NOPS"
	exit $STATE_UNKNOWN
fi

if [ $CURRENT -ge $CRITICAL ]; then
	$ECHO "Skynet untaken_tasks: $CURRENT threshold: $CRITICAL   CRITICAL"
	$ECHO " "
    exit $STATE_CRITICAL
fi

if [ $CURRENT -ge $WARNING ]; then
	$ECHO "Skynet untaken_tasks: $CURRENT threshold: $WARNING   WARNING"
	$ECHO " "
    exit $STATE_WARNING
fi

if [[ $CURRENT -lt $CRITICAL  &&  $CURRENT -lt $WARNING ]]; then
	$ECHO "Skynet untaken_tasks: $CURRENT threshold critical: $CRITICAL threshold warning: $WARNING OK"
	$ECHO " "
    exit $STATE_OK
	
fi
$ECHO "NO SCRIPT OUTPUT CALL NOPS!"
exit $STATE_CRITICAL
