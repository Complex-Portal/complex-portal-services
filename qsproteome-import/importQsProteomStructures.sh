#!/bin/bash

#SBATCH --time=06-00:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=5   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH -p production   # partition(s)
#SBATCH --mem=16G   # memory per node
#SBATCH -o "/nfs/production/hhe/intact/data/db-import-logs/import-qs-proteome-structures-%j.out"   # job output file
#SBATCH --mail-user=jmedina@ebi.ac.uk   # email address
#SBATCH --mail-type=ALL

if [ $# -ne 5 ]; then
      echo ""
      echo "ERROR: wrong number of parameters ($#)."
      echo "usage: $0 PROFILE USER_ID OUTPUT_DIRECTORY SEPARATOR[',', '\t'] HEADER[true, false]"
      echo ""
      exit 1
fi

PROFILE=$1
USER_ID=$2
OUTPUT_DIRECTORY=$3
SEPARATOR=$4
HEADER=$5
INPUT_FILE=ignore

echo "Profile: $PROFILE"
echo "User id: $USER_ID"
echo "Output directory: $OUTPUT_DIRECTORY"
echo "Separator: $SEPARATOR"
echo "Header: $HEADER"

mvn clean -U install -P import-qsproteome-structures,${PROFILE} -Djami.user.context.id=${USER_ID} -Djob.name=complexQsProteomeImport -Dinput.file.name=$INPUT_FILE -Doutput.directory=$OUTPUT_DIRECTORY -Dseparator=$SEPARATOR -Dheader=$HEADER -DskipTests
