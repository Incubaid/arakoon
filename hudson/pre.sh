set -e
set -u

for STEP in `ls ./hudson/pre/*.sh`;
do
    chmod u+x ${STEP};
    ./${STEP};
done
