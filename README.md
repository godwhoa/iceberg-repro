# Steps to re-produce metadata corruption


```bash

❯ docker compose up -d

❯ go run main.go
Uploading to bucket: warehouse key: test/ecs/data/date_partition=2024-12-03/efdbed15-b5ac-4eb7-bd30-c6ef038f8235.parquet
Added s3://warehouse/test/ecs/data/date_partition=2024-12-03/efdbed15-b5ac-4eb7-bd30-c6ef038f8235.parquet to iceberg table and commited.

❯ docker exec -it spark-iceberg spark-sql
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/05/20 07:58:57 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
25/05/20 07:58:57 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
Spark Web UI available at http://7f3b80637cd7:4041
Spark master: local[*], Application Id: local-1747727937727
spark-sql ()> SHOW CREATE TABLE test.ecs;
CREATE TABLE demo.test.ecs (
  id STRING NOT NULL,
  identity STRING,
  `@timestamp` TIMESTAMP_NTZ NOT NULL)
USING iceberg
PARTITIONED BY (days(`@timestamp`))
LOCATION 's3://warehouse/test/ecs'
TBLPROPERTIES (
  'current-snapshot-id' = '7620377911139510457',
  'format' = 'iceberg/parquet',
  'format-version' = '2',
  'schema.name-mapping.default' = '[{"names":["id"],"field-id":1},{"names":["identity"],"field-id":2},{"names":["@timestamp"],"field-id":3}]',
  'write.parquet.compression-codec' = 'zstd')

Time taken: 0.55 seconds, Fetched 1 row(s)
spark-sql ()> SELECT COUNT(1) FROM test.ecs;
25/05/20 07:59:21 ERROR SparkSQLDriver: Failed in [SELECT COUNT(1) FROM test.ecs]
java.lang.ArrayStoreException: org.apache.iceberg.shaded.org.apache.avro.generic.GenericData$Record
        at java.base/java.util.LinkedList.toArray(LinkedList.java:1107)
        at org.apache.iceberg.GenericManifestFile.internalSet(GenericManifestFile.java:415)
        at org.apache.iceberg.avro.SupportsIndexProjection.set(SupportsIndexProjection.java:83)
        at org.apache.iceberg.GenericManifestFile.put(GenericManifestFile.java:427)
        at org.apache.iceberg.avro.ValueReaders$PlannedIndexedReader.set(ValueReaders.java:1013)
        at org.apache.iceberg.avro.ValueReaders$PlannedIndexedReader.set(ValueReaders.java:979)
        at org.apache.iceberg.avro.ValueReaders$PlannedStructReader.read(ValueReaders.java:933)
        at org.apache.iceberg.avro.GenericAvroReader.read(GenericAvroReader.java:99)
        at org.apache.iceberg.avro.NameMappingDatumReader.read(NameMappingDatumReader.java:57)
        at org.apache.iceberg.shaded.org.apache.avro.file.DataFileStream.next(DataFileStream.java:264)
        at org.apache.iceberg.shaded.org.apache.avro.file.DataFileStream.next(DataFileStream.java:249)
        at org.apache.iceberg.io.CloseableIterator$1.next(CloseableIterator.java:55)
        at org.apache.iceberg.relocated.com.google.common.collect.Iterators.addAll(Iterators.java:368)
        at org.apache.iceberg.relocated.com.google.common.collect.Iterables.addAll(Iterables.java:333)
        at org.apache.iceberg.relocated.com.google.common.collect.Lists.newLinkedList(Lists.java:260)
        at org.apache.iceberg.ManifestLists.read(ManifestLists.java:45)
        at org.apache.iceberg.BaseSnapshot.cacheManifests(BaseSnapshot.java:165)
        at org.apache.iceberg.BaseSnapshot.deleteManifests(BaseSnapshot.java:199)
        at org.apache.iceberg.BaseDistributedDataScan.findMatchingDeleteManifests(BaseDistributedDataScan.java:208)
        at org.apache.iceberg.BaseDistributedDataScan.doPlanFiles(BaseDistributedDataScan.java:149)
        at org.apache.iceberg.SnapshotScan.planFiles(SnapshotScan.java:139)
        at org.apache.iceberg.spark.source.SparkScanBuilder.pushAggregation(SparkScanBuilder.java:240)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.org$apache$spark$sql$execution$datasources$v2$V2ScanRelationPushDown$$rewriteAggregate(V2ScanRelationPushDown.scala:177)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$$anonfun$pushDownAggregates$1.applyOrElse(V2ScanRelationPushDown.scala:97)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$$anonfun$pushDownAggregates$1.applyOrElse(V2ScanRelationPushDown.scala:95)
        at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)
        at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transform(TreeNode.scala:405)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.pushDownAggregates(V2ScanRelationPushDown.scala:95)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.$anonfun$apply$4(V2ScanRelationPushDown.scala:46)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.$anonfun$apply$8(V2ScanRelationPushDown.scala:52)
        at scala.collection.LinearSeqOptimized.foldLeft(LinearSeqOptimized.scala:126)
        at scala.collection.LinearSeqOptimized.foldLeft$(LinearSeqOptimized.scala:122)
        at scala.collection.immutable.List.foldLeft(List.scala:91)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.apply(V2ScanRelationPushDown.scala:51)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.apply(V2ScanRelationPushDown.scala:38)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$2(RuleExecutor.scala:222)
        at scala.collection.LinearSeqOptimized.foldLeft(LinearSeqOptimized.scala:126)
        at scala.collection.LinearSeqOptimized.foldLeft$(LinearSeqOptimized.scala:122)
        at scala.collection.immutable.List.foldLeft(List.scala:91)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:219)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1$adapted(RuleExecutor.scala:211)
        at scala.collection.immutable.List.foreach(List.scala:431)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:211)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$executeAndTrack$1(RuleExecutor.scala:182)
        at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:89)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.executeAndTrack(RuleExecutor.scala:182)
        at org.apache.spark.sql.execution.QueryExecution.$anonfun$optimizedPlan$1(QueryExecution.scala:152)
        at org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:138)
        at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$2(QueryExecution.scala:219)
        at org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:546)
        at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:219)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
        at org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:218)
        at org.apache.spark.sql.execution.QueryExecution.optimizedPlan$lzycompute(QueryExecution.scala:148)
        at org.apache.spark.sql.execution.QueryExecution.optimizedPlan(QueryExecution.scala:144)
        at org.apache.spark.sql.execution.QueryExecution.assertOptimized(QueryExecution.scala:162)
        at org.apache.spark.sql.execution.QueryExecution.executedPlan$lzycompute(QueryExecution.scala:182)
        at org.apache.spark.sql.execution.QueryExecution.executedPlan(QueryExecution.scala:179)
        at org.apache.spark.sql.execution.QueryExecution.simpleString(QueryExecution.scala:238)
        at org.apache.spark.sql.execution.QueryExecution.org$apache$spark$sql$execution$QueryExecution$$explainString(QueryExecution.scala:284)
        at org.apache.spark.sql.execution.QueryExecution.explainString(QueryExecution.scala:252)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:117)
        at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
        at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLDriver.run(SparkSQLDriver.scala:76)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.processCmd(SparkSQLCLIDriver.scala:501)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.$anonfun$processLine$1(SparkSQLCLIDriver.scala:619)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.$anonfun$processLine$1$adapted(SparkSQLCLIDriver.scala:613)
        at scala.collection.Iterator.foreach(Iterator.scala:943)
        at scala.collection.Iterator.foreach$(Iterator.scala:943)
        at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
        at scala.collection.IterableLike.foreach(IterableLike.scala:74)
        at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
        at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.processLine(SparkSQLCLIDriver.scala:613)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver$.main(SparkSQLCLIDriver.scala:310)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.main(SparkSQLCLIDriver.scala)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.base/java.lang.reflect.Method.invoke(Method.java:569)
        at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
        at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:1034)
        at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:199)
        at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:222)
        at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:91)
        at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1125)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1134)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
org.apache.iceberg.shaded.org.apache.avro.generic.GenericData$Record
java.lang.ArrayStoreException: org.apache.iceberg.shaded.org.apache.avro.generic.GenericData$Record
        at java.base/java.util.LinkedList.toArray(LinkedList.java:1107)
        at org.apache.iceberg.GenericManifestFile.internalSet(GenericManifestFile.java:415)
        at org.apache.iceberg.avro.SupportsIndexProjection.set(SupportsIndexProjection.java:83)
        at org.apache.iceberg.GenericManifestFile.put(GenericManifestFile.java:427)
        at org.apache.iceberg.avro.ValueReaders$PlannedIndexedReader.set(ValueReaders.java:1013)
        at org.apache.iceberg.avro.ValueReaders$PlannedIndexedReader.set(ValueReaders.java:979)
        at org.apache.iceberg.avro.ValueReaders$PlannedStructReader.read(ValueReaders.java:933)
        at org.apache.iceberg.avro.GenericAvroReader.read(GenericAvroReader.java:99)
        at org.apache.iceberg.avro.NameMappingDatumReader.read(NameMappingDatumReader.java:57)
        at org.apache.iceberg.shaded.org.apache.avro.file.DataFileStream.next(DataFileStream.java:264)
        at org.apache.iceberg.shaded.org.apache.avro.file.DataFileStream.next(DataFileStream.java:249)
        at org.apache.iceberg.io.CloseableIterator$1.next(CloseableIterator.java:55)
        at org.apache.iceberg.relocated.com.google.common.collect.Iterators.addAll(Iterators.java:368)
        at org.apache.iceberg.relocated.com.google.common.collect.Iterables.addAll(Iterables.java:333)
        at org.apache.iceberg.relocated.com.google.common.collect.Lists.newLinkedList(Lists.java:260)
        at org.apache.iceberg.ManifestLists.read(ManifestLists.java:45)
        at org.apache.iceberg.BaseSnapshot.cacheManifests(BaseSnapshot.java:165)
        at org.apache.iceberg.BaseSnapshot.deleteManifests(BaseSnapshot.java:199)
        at org.apache.iceberg.BaseDistributedDataScan.findMatchingDeleteManifests(BaseDistributedDataScan.java:208)
        at org.apache.iceberg.BaseDistributedDataScan.doPlanFiles(BaseDistributedDataScan.java:149)
        at org.apache.iceberg.SnapshotScan.planFiles(SnapshotScan.java:139)
        at org.apache.iceberg.spark.source.SparkScanBuilder.pushAggregation(SparkScanBuilder.java:240)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.org$apache$spark$sql$execution$datasources$v2$V2ScanRelationPushDown$$rewriteAggregate(V2ScanRelationPushDown.scala:177)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$$anonfun$pushDownAggregates$1.applyOrElse(V2ScanRelationPushDown.scala:97)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$$anonfun$pushDownAggregates$1.applyOrElse(V2ScanRelationPushDown.scala:95)
        at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)
        at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transform(TreeNode.scala:405)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.pushDownAggregates(V2ScanRelationPushDown.scala:95)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.$anonfun$apply$4(V2ScanRelationPushDown.scala:46)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.$anonfun$apply$8(V2ScanRelationPushDown.scala:52)
        at scala.collection.LinearSeqOptimized.foldLeft(LinearSeqOptimized.scala:126)
        at scala.collection.LinearSeqOptimized.foldLeft$(LinearSeqOptimized.scala:122)
        at scala.collection.immutable.List.foldLeft(List.scala:91)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.apply(V2ScanRelationPushDown.scala:51)
        at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$.apply(V2ScanRelationPushDown.scala:38)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$2(RuleExecutor.scala:222)
        at scala.collection.LinearSeqOptimized.foldLeft(LinearSeqOptimized.scala:126)
        at scala.collection.LinearSeqOptimized.foldLeft$(LinearSeqOptimized.scala:122)
        at scala.collection.immutable.List.foldLeft(List.scala:91)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:219)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1$adapted(RuleExecutor.scala:211)
        at scala.collection.immutable.List.foreach(List.scala:431)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:211)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$executeAndTrack$1(RuleExecutor.scala:182)
        at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:89)
        at org.apache.spark.sql.catalyst.rules.RuleExecutor.executeAndTrack(RuleExecutor.scala:182)
        at org.apache.spark.sql.execution.QueryExecution.$anonfun$optimizedPlan$1(QueryExecution.scala:152)
        at org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:138)
        at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$2(QueryExecution.scala:219)
        at org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:546)
        at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:219)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
        at org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:218)
        at org.apache.spark.sql.execution.QueryExecution.optimizedPlan$lzycompute(QueryExecution.scala:148)
        at org.apache.spark.sql.execution.QueryExecution.optimizedPlan(QueryExecution.scala:144)
        at org.apache.spark.sql.execution.QueryExecution.assertOptimized(QueryExecution.scala:162)
        at org.apache.spark.sql.execution.QueryExecution.executedPlan$lzycompute(QueryExecution.scala:182)
        at org.apache.spark.sql.execution.QueryExecution.executedPlan(QueryExecution.scala:179)
        at org.apache.spark.sql.execution.QueryExecution.simpleString(QueryExecution.scala:238)
        at org.apache.spark.sql.execution.QueryExecution.org$apache$spark$sql$execution$QueryExecution$$explainString(QueryExecution.scala:284)
        at org.apache.spark.sql.execution.QueryExecution.explainString(QueryExecution.scala:252)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:117)
        at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
        at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLDriver.run(SparkSQLDriver.scala:76)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.processCmd(SparkSQLCLIDriver.scala:501)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.$anonfun$processLine$1(SparkSQLCLIDriver.scala:619)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.$anonfun$processLine$1$adapted(SparkSQLCLIDriver.scala:613)
        at scala.collection.Iterator.foreach(Iterator.scala:943)
        at scala.collection.Iterator.foreach$(Iterator.scala:943)
        at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
        at scala.collection.IterableLike.foreach(IterableLike.scala:74)
        at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
        at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.processLine(SparkSQLCLIDriver.scala:613)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver$.main(SparkSQLCLIDriver.scala:310)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.main(SparkSQLCLIDriver.scala)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.base/java.lang.reflect.Method.invoke(Method.java:569)
        at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
        at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:1034)
        at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:199)
        at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:222)
        at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:91)
        at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1125)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1134)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
spark-sql ()> 
```