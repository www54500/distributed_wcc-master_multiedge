# distributed_wcc for multi-edge
The implementation of the distributed community detection algorithm for optimizing the WCC metric

Example of how to run:
```
WCC_OPTIONS="-ca wcc.maxRetries=2" # Number of retries with minimal WCC improvement before algorithm halts
GIRAPH_OPTIONS="-ca giraph.useSuperstepCounters=false -ca giraph.numComputeThreads=$N_THREADS -ca giraph.numInputThreads=$N_THREADS -ca giraph.numOutputThreads=$N_THREADS -ca giraph.oneToAllMsgSending=true -ca giraph.userPartitionCount=$N_PARTITIONS -ca giraph.outEdgesClass=utils.IntNullArrayEdges"

$HADOOP_HOME/bin/hadoop --config $CONF jar $GIR_JAR org.apache.giraph.GiraphRunner -D 'mapred.child.java.opts=-Xms4G -Xmx8G' -D-Xmx8192m -libjars $LIBJARS computation.StartComputation $GIRAPH_OPTIONS $WCC_OPTIONS -eif org.apache.giraph.io.formats.IntNullReverseTextEdgeInputFormat -eip $INPUT -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op $OUTPUT -w $N_WORKERS -mc computation.WccMasterCompute

```

