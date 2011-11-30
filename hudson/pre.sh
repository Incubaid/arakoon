set -e
set -u

for STEP in `ls pre/*.sh`;
do
    chmod u+x ${STEP};
    ./${STEP};
done
