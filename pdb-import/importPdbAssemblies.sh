#!/bin/bash

#SBATCH --time=06-00:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=5   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH -p production   # partition(s)
#SBATCH --mem=16G   # memory per node
#SBATCH -o "/nfs/production/hhe/intact/data/db-import-logs/import-pdb-assemblies-%j.out"   # job output file
#SBATCH --mail-user=jmedina@ebi.ac.uk   # email address
#SBATCH --mail-type=ALL

if [ $# -ne 6 ]; then
      echo ""
      echo "ERROR: wrong number of parameters ($#)."
      echo "usage: $0 PROFILE USER_ID INPUT_FILE OUTPUT_DIRECTORY SEPARATOR[',', '\t'] HEADER[true, false]"
      echo ""
      exit 1
fi

PROFILE=$1;
USER_ID=$2;
INPUT_FILE=$3
OUTPUT_DIRECTORY=$4
SEPARATOR=$5
HEADER=$6

echo "Profile: $PROFILE"
echo "User id: $USER_ID"
echo "Input file: $INPUT_FILE"
echo "Output directory: $OUTPUT_DIRECTORY"
echo "Separator: $SEPARATOR"
echo "Header: $HEADER"

mvn clean -U install -P import-pdb-assemblies,${PROFILE} -Djami.user.context.id=${USER_ID} -Djob.name=pdbAssembliesImport -Dinput.file.name=$INPUT_FILE -Doutput.directory=$OUTPUT_DIRECTORY -Dseparator=$SEPARATOR -Dheader=$HEADER -DskipTests
