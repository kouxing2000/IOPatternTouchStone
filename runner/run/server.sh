#!/bin/sh

#############################################################################
# 
#############################################################################

if [ x$JAVA_HOME = "x" ]; then
        echo  please setup JAVA_HOME
        exit 1
fi

JAVA_BIN=$JAVA_HOME/bin/java

BIN_PATH=$(cd "$(dirname "$0")"; pwd)
LIB=$BIN_PATH/lib

if [ "$LIB" == "" -o ! -d "$LIB" ]; then
        echo "The lib home: $LIB is NOT exist in your system."
        exit 1
fi

LIBS=$(ls $LIB)

CLASSPATH=.:$LIB

for lib in $LIBS
do
        CLASSPATH=$CLASSPATH:${LIB}/${lib}
done

exec_info="$JAVA_BIN -server -Xmx512m -Xms512m -XX:MaxNewSize=64m -XX:NewSize=64m -XX:+UseConcMarkSweepGC -classpath "$CLASSPATH:" VLServer $*"

echo $exec_info

$exec_info 

