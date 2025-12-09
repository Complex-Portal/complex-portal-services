#!/bin/bash

#SBATCH --time=12:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=5   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH -p production   # partition(s)
#SBATCH --mem=4G   # memory per node
#SBATCH -o "/nfs/production/hhe/intact/data/complex-job-logs/check-complex-orthologs-%j.out"   # job output file
#SBATCH --mail-user=intact-dev@ebi.ac.uk   # email address
#SBATCH --mail-type=ALL

if [ $# -ne 7 ]; then
      echo ""
      echo "ERROR: wrong number of parameters ($#)."
      echo "usage: $0 PROFILE USER_ID INPUT_TAX_ID OUTPUT_TAX_ID OUTPUT_DIRECTORY SEPARATOR[',', '\t'] HEADER[true, false]"
      echo ""
      exit 1
fi

PROFILE=$1
USER_ID=$2
INPUT_TAX_ID=$3
OUTPUT_TAX_ID=$4
OUTPUT_DIRECTORY=$5
SEPARATOR=$6
HEADER=$7

echo "Profile: $PROFILE"
echo "User id: $USER_ID"
echo "Input tax id: $INPUT_TAX_ID"
echo "Output tax id: $OUTPUT_TAX_ID"
echo "Output directory: $OUTPUT_DIRECTORY"
echo "Separator: $SEPARATOR"
echo "Header: $HEADER"

MAVEN_OPTS="$MAVEN_OPTS -Dmaven.wagon.http.ssl.insecure=true"
MAVEN_OPTS="$MAVEN_OPTS -Dmaven.wagon.http.ssl.allowall=true"

mvn clean install -P check-complex-orthologs,${PROFILE} -Djami.user.context.id=${USER_ID} -Djob.name=checkOrthologsJob -Dinput.tax.id=$INPUT_TAX_ID -Doutput.tax.id=$OUTPUT_TAX_ID -Doutput.directory=$OUTPUT_DIRECTORY -Dseparator=$SEPARATOR -Dheader=$HEADER -DskipTests
