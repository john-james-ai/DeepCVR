cd $SPARK_HOME
./sbin/start-master.sh
SPARK_WORKER_INSTANCES=4
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=8G
./sbin/start-worker.sh spark://Leviathan.localdomain:7077