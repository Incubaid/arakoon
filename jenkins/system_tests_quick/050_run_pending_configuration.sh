#!/bin/bash -xue

export ARAKOON_PYTHON_CLIENT=pyrakoon

/opt/qbase3/qshell -c "
q.qp._runPendingReconfigeFiles()
"
