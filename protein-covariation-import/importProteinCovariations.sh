#!/bin/bash

#SBATCH --time=2-00:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=5   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH -p production   # partition(s)
#SBATCH --mem=8G   # memory per node
#SBATCH -J "COVARIATION_IMPORT"   # job name
#SBATCH -o "/nfs/production/hhe/intact/data/protein-covariation-import-logs/import-protein-covariations-%j.out"   # job output file
#SBATCH --mail-user=jmedina@ebi.ac.uk   # email address
#SBATCH --mail-type=ALL

if [ $# -ne 5 ]; then
      echo ""
      echo "ERROR: wrong number of parameters ($#)."
      echo "usage: $0 OUTPUT_DIRECTORY OUTPUT_DIR_NAME DATABASE_ID SEPARATOR[',', '\t'] HEADER[true, false]"
      echo ""
      exit 1
fi

INPUT_FILE=ignored
OUTPUT_DIRECTORY=$1
OUTPUT_DIR_NAME=$2
DATABASE_ID=$3
SEPARATOR=$4
HEADER=$5

mvn clean -U install -P run-protein-covariation-job -Djob.name=importProteinCovariationsJob -Dinput.file.name=$INPUT_FILE -Dprocess.output.dir.name=$OUTPUT_DIR_NAME -Doutput.directory=$OUTPUT_DIRECTORY -Ddatabase.id=$DATABASE_ID -Dseparator=$SEPARATOR -Dheader=$HEADER -DskipTests

