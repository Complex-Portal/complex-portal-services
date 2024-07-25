#!/bin/bash

if [ $# -ne 6 ]; then
      echo ""
      echo "ERROR: wrong number of parameters ($#)."
      echo "usage: $0 PROFILE INPUT_FILE OUTPUT_FILE_NAME OUTPUT_DIRECTORY SEPARATOR[',', '\t'] HEADER[true, false]"
      echo ""
      exit 1
fi

PROFILE=$1;
INPUT_FILE=$2
OUTPUT_FILE_NAME=$3
OUTPUT_DIRECTORY=$4
SEPARATOR=$5
HEADER=$6

echo "Profile: $PROFILE"
echo "Input file: $INPUT_FILE"
echo "Output file: $OUTPUT_FILE_NAME"
echo "Output directory: $OUTPUT_DIRECTORY"
echo "Separator: $SEPARATOR"
echo "Header: $HEADER"

mvn clean -U install -P import-uniplex-clusters,${PROFILE} -Djob.name=uniplexClusterImport -Dinput.file.name=$INPUT_FILE -Doutput.file.name=$OUTPUT_FILE_NAME -Doutput.directory=$OUTPUT_DIRECTORY -Dseparator=$SEPARATOR -Dheader=$HEADER -DskipTests
