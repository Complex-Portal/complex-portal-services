#!/bin/bash

#SBATCH --time=12:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=5   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH -p production   # partition(s)
#SBATCH --mem=4G   # memory per node
#SBATCH -o "/homes/mi/test-complex-intact-coverage/check-complex-intact-coverage-%j.out"   # job output file
#SBATCH --mail-user=jmedina@ebi.ac.uk   # email address
#SBATCH --mail-type=ALL

#if [ $# -ne 7 ]; then
#      echo ""
#      echo "ERROR: wrong number of parameters ($#)."
#      echo "usage: $0 PROFILE USER_ID TAX_ID INTACT_GRAPH_WS_URL OUTPUT_DIRECTORY SEPARATOR[',', '\t'] HEADER[true, false]"
#      echo ""
#      exit 1
#fi

PROFILE=ebi-test
USER_ID=jmedina
TAX_ID=9606
INTACT_GRAPH_WS_URL=https://wwwdev.ebi.ac.uk/intact/ws/graph/
OUTPUT_DIRECTORY=/Users/jmedina/Downloads/test-complex-intact-coverage
SEPARATOR=,
HEADER=true

echo "Profile: $PROFILE"
echo "User id: $USER_ID"
echo "Tax id: $TAX_ID"
echo "IntAct Graph WS URL: $INTACT_GRAPH_WS_URL"
echo "Output directory: $OUTPUT_DIRECTORY"
echo "Separator: $SEPARATOR"
echo "Header: $HEADER"

mvn clean -U install -P check-complex-intact-coverage,${PROFILE} -Djami.user.context.id=${USER_ID} -Djob.name=checkIntactCoverageJob -Dtax.id=$TAX_ID -Dintact.graph.ws.url=$INTACT_GRAPH_WS_URL -Doutput.directory=$OUTPUT_DIRECTORY -Dseparator=$SEPARATOR -Dheader=$HEADER -DskipTests
