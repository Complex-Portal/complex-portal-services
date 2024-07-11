#!/bin/bash

if [ $# -ne 4 ]; then
      echo ""
      echo "ERROR: wrong number of parameters ($#)."
      echo "usage: $0 INPUT_FILE OUTPUT_FILE_NAME SEPARATOR[',', '\t'] HEADER[true, false]"
      echo ""
      exit 1
fi

INPUT_FILE=$1
OUTPUT_FILE_NAME=$2
SEPARATOR=$3
HEADER=$4

mvn clean -U install -P process-uniplex-file -DinputFileName=$INPUT_FILE -DoutputFileName=$OUTPUT_FILE_NAME -Dseparator=$SEPARATOR -Dheader=$HEADER -Dmaven.test.skip
