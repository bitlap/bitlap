# Set Bitlap-specific environment variables here.

# The java implementation to use.
export JAVA_HOME=${JAVA_HOME}

export BITLAP_CONF_DIR=${BITLAP_CONF_DIR:-"$BITLAP_HOME/conf"}
export BITLAP_CLASSPATH="$CLASSPATH:$BITLAP_CONF_DIR:$BITLAP_HOME/lib/*"
# Extra Java runtime options.  Empty by default.
export BITLAP_OPTS="-Djava.net.preferIPv4Stack=true $BITLAP_OPTS"

# Command specific options appended to BITLAP_OPTS when specified
JDK11="--add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
export BITLAP_SERVER_OPTS="-Xmx1024m $BITLAP_SERVER_OPTS $BITLAP_OPTS $JDK11"

# The following applies to multiple commands (sql, ...)
export BITLAP_CLIENT_OPTS="-Xmx512m $BITLAP_CLIENT_OPTS $BITLAP_OPTS"

# Where log files are stored.  $BITLAP_HOME/logs by default.
export BITLAP_LOG_DIR=${BITLAP_LOG_DIR:-"$BITLAP_HOME/logs"}

# The directory where pid files are stored. $BITLAP_HOME/tmp by default.
# NOTE: this should be set to a directory that can only be written to by
#       the user that will run the bitlap servers.  Otherwise there is the
#       potential for a symlink attack.
export BITLAP_TMP_DIR=${BITLAP_TMP_DIR:-"$BITLAP_HOME/tmp"}

# Attempt to set JAVA_HOME if it is not set
if [[ -z $JAVA_HOME ]]; then
  # On OSX use java_home (or /Library for older versions)
  if [ "Darwin" == "$(uname -s)" ]; then
    if [ -x /usr/libexec/java_home ]; then
      export JAVA_HOME=($(/usr/libexec/java_home))
    else
      export JAVA_HOME=(/Library/Java/Home)
    fi
  fi
  # Fail if we did not detect it
  if [[ -z $JAVA_HOME ]]; then
     echo "Error: JAVA_HOME is not set and could not be found." 1>&2
     exit 1
   fi
 fi
JAVA=$JAVA_HOME/bin/java
# JAVA=`whereis java`
