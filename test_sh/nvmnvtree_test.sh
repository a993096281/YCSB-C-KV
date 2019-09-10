#/bin/bash

workload="workloads/workloada.spec"
dbpath="/home/lzw/ceshi2"
moreworkloads="workloads/workloada.spec:workloads/workloadb.spec:workloads/workloadc.spec:workloads/workloadd.spec:workloads/workloade.spec:workloads/workloadf.spec"

#./ycsbc -db nvmnvtree -dbpath $dbpath -threads 1 -P $workload -load true -run true -dbstatistics true

#./ycsbc -db nvmnvtree -dbpath $dbpath -threads 1 -P $workload -load true -morerun $moreworkloads -dbstatistics true

cmd="./ycsbc -db nvmnvtree -dbpath $dbpath -threads 1 -P $workload -load true -morerun $moreworkloads -dbstatistics true "
echo $cmd
eval $cmd