#!/bin/sh


basepath=`readlink -f $1`
if [ $? -ne 0 ]; then
basepath=`readlink -f $(dirname $0)`
fi
echo "No path specified, using ${basepath}"

set -e
cd ${basepath}
for i in `ls ${basepath}/*.py`; do
    redis-cli flushdb
    python $i
done
