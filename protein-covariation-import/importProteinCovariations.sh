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

if [ $# -ne 4 ]; then
      echo ""
      echo "ERROR: wrong number of parameters ($#)."
      echo "usage: $0 INPUT_FILE OUTPUT_DIRECTORY SEPARATOR[',', '\t'] HEADER[true, false]"
      echo ""
      exit 1
fi

INPUT_FILE=$1
OUTPUT_DIR_NAME=$1
OUTPUT_DIRECTORY=$2
SEPARATOR=$3
HEADER=$4

mvn clean -U install -P run-protein-covariation-job -Djob.name=importProteinCovariationsJob -Dinput.file.name=$INPUT_FILE -Dprocess.output.dir.name=$OUTPUT_DIR_NAME -Doutput.directory=$OUTPUT_DIRECTORY -Dseparator=$SEPARATOR -Dheader=$HEADER -DskipTests

