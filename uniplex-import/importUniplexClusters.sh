#!/bin/bash

if [ $# -ne 5 ]; then
      echo ""
      echo "ERROR: wrong number of parameters ($#)."
      echo "usage: $0 PROFILE INPUT_FILE OUTPUT_DIRECTORY SEPARATOR[',', '\t'] HEADER[true, false]"
      echo ""
      exit 1
fi

PROFILE=$1;
INPUT_FILE=$2
OUTPUT_DIRECTORY=$3
SEPARATOR=$4
HEADER=$5

echo "Profile: $PROFILE"
echo "Input file: $INPUT_FILE"
echo "Output directory: $OUTPUT_DIRECTORY"
echo "Separator: $SEPARATOR"
echo "Header: $HEADER"

mvn clean -U install -P import-uniplex-clusters,${PROFILE} -Djob.name=uniplexClusterImport -Dinput.file=$INPUT_FILE -Doutput.directory=$OUTPUT_DIRECTORY -Dseparator=$SEPARATOR -Dheader=$HEADER -Dmaven.test.skip
