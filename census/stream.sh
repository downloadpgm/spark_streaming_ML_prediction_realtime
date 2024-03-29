# send 10 records each time to hdfs
cont=0
while [ $cont -lt 30 ]; do
   interv_ini=$((cont * 10))
   interv_fim=$(((cont + 1) * 10))
   awk -v vini=${interv_ini} -v vfim=${interv_fim} '(NR>vini && NR<=vfim)' adult.data >head${cont}.csv
   hdfs dfs -put head${cont}.csv /data/stream
   cont=`expr $cont + 1`
   echo Loop $cont
   sleep 10
done