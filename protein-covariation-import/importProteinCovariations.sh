#!/bin/bash

#SBATCH --time=2-00:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=5   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH -p production   # partition(s)
#SBATCH --mem=8G   # memory per node
#SBATCH -J "COVARIATION_IMPORT"   # job name
#SBATCH -o "/nfs/production/hhe/intact/data/protein-covariation-import-logs/import-protein-covariations-%j.out"   # job output file
#SBATCH --mail-user=intact-dev@ebi.ac.uk   # email address
#SBATCH --mail-type=ALL

if [ $# -ne 7 ]; then
      echo ""
      echo "ERROR: wrong number of parameters ($#)."
      echo "usage: $0 PROFILE USER_ID OUTPUT_DIRECTORY OUTPUT_DIR_NAME DATABASE_ID SEPARATOR[',', '\t'] HEADER[true, false]"
      echo ""
      exit 1
fi

PROFILE=$1
USER_ID=$2
INPUT_FILE=ignored
OUTPUT_DIRECTORY=$3
OUTPUT_DIR_NAME=$4
DATABASE_ID=$5
SEPARATOR=$6
HEADER=$7

mvn clean -U install -P run-protein-covariation-job,${PROFILE} -Djob.name=importProteinCovariationsJob -Dinput.file.name=$INPUT_FILE -Dprocess.output.dir.name=$OUTPUT_DIR_NAME -Doutput.directory=$OUTPUT_DIRECTORY -Ddatabase.id=$DATABASE_ID -Dseparator=$SEPARATOR -Dheader=$HEADER -Djami.user.context.id=${USER_ID} -DskipTests

