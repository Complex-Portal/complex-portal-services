#!/bin/bash

if [ $# -ne 3 ]; then
      echo ""
      echo "ERROR: wrong number of parameters ($#)."
      echo "usage: $0 PROFILE INPUT_FILE OUTPUT_DIRECTORY"
      echo ""
      exit 1
fi

PROFILE=$1;
INPUT_FILE=$2
OUTPUT_DIRECTORY=$3

echo "Profile: $PROFILE"
echo "Input file: $INPUT_FILE"
echo "Output directory: $OUTPUT_DIRECTORY"

mvn clean -U install -P import-uniplex-clusters,${PROFILE} -Djob.name=uniplexClusterImport -Dinput.file=$INPUT_FILE -Doutput.directory=$OUTPUT_DIRECTORY -Dmaven.test.skip
