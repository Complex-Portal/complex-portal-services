#!/bin/bash

if [ $# -ne 5 ]; then
      echo ""
      echo "ERROR: wrong number of parameters ($#)."
      echo "usage: $0 INPUT_FILE OUTPUT_FILE_NAME REPORT_DIR SEPARATOR[',', '\t'] HEADER[true, false]"
      echo ""
      exit 1
fi

INPUT_FILE=$1
OUTPUT_FILE_NAME=$2
REPORT_DIR=$3
SEPARATOR=$4
HEADER=$5

MAVEN_OPTS="$MAVEN_OPTS -Dmaven.wagon.http.ssl.insecure=true"
MAVEN_OPTS="$MAVEN_OPTS -Dmaven.wagon.http.ssl.allowall=true"

mvn clean install -P process-music-file -Dinput.file.name=$INPUT_FILE -Doutput.file.name=$OUTPUT_FILE_NAME -Doutput.directory=$REPORT_DIR -Dseparator=$SEPARATOR -Dheader=$HEADER -DskipTests
