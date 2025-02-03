#!/bin/bash

#if [ $# -ne 5 ]; then
#      echo ""
#      echo "ERROR: wrong number of parameters ($#)."
#      echo "usage: $0 INPUT_FILE OUTPUT_FILE_NAME REPORT_DIR SEPARATOR[',', '\t'] HEADER[true, false]"
#      echo ""
#      exit 1
#fi

INPUT_FILE=/Users/jmedina/Downloads/ProHD2_covariation/out/datasets/RF_predictions.csv
OUTPUT_FILE_NAME=output_temp_1
REPORT_DIR=/Users/jmedina/Downloads/ProHD2_covariation/test_import
SEPARATOR=,
HEADER=true

mvn clean -U install -P run-protein-covariation-job -Djob.name=processProteinCovariationFileJob -Dinput.file.name=$INPUT_FILE -Doutput.file.name=$OUTPUT_FILE_NAME -Doutput.directory=$REPORT_DIR -Dseparator=$SEPARATOR -Dheader=$HEADER -DskipTests
