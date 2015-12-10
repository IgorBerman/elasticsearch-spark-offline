# Elasticsearch offline snapshot preparation with spark

## based on [elasticsearch-lambda project](https://github.com/MyPureCloud/elasticsearch-lambda)

## Is it production ready? - Almost, you should test yourself

## How it works: 
1. It takes spark rdd, and for each partition creates local elasticsearch index within same worker jvm with number of shards == 1 
2. Bulk uploads partition data to this local index and snapshots it 
3. Uploads snapshot to destination under where shard data should be located. 
4. Creates empty index with final number of shards and snapshots it too(done on driver)
5. Uploads metadata of the last snapshot to destination. Thus data of snapshot is created from rdd and metadata of snapshot is created on driver

## How to use
Check test for full example, in general you configure different parts of pipeline that will create and upload snapshot and use spark context to process it,e.g.
  ```
  //provide guava supplier that is serializable(it will be passed to workers and should create hadoop configuration with proper credentials and other settings)
  com.google.common.base.Supplier<org.apache.hadoop.conf.Configuration> configurationSupplier = ...;
  Map<String, String> additionalEsSettigns = new HashMap<>();
  additionalEsSettigns.put("es.mapping.exclude", "excludedField");
						
  ESFilesTransport transport = new ESFilesTransport();
  InputStream is = getClass().getResourceAsStream("/es-template.json");
  String templateContent = CharStreams.toString(new InputStreamReader(is));
  final ESIndexShardSnapshotCreator creator = new ESIndexShardSnapshotCreator(
					transport, 
					additionalEsSettigns,
					"/tmp/snapshotsWorkingDir/",
					"s3a://path-of-snapshots",	//prefix will define hadoop filesystem that will be used to upload snapshot to destination dir(hdfs://, s3://, file:// etc)				 
					"snapshot-repo-name", 
					"/tmp/esWorkingDir",  
					"template-name",
					templateContent, 
					10, 
					1024);
 final String indexName = "my-index";//make it different each time
 final String indexType = "type";    //should be consistent with mapping defined in es-template.json
 int bulkSize = 10000;
 long timeout = 10000;
 ESIndexShardSnapshotPipeline<String, MyEsData> pipeline = new ESIndexShardSnapshotPipeline<>(creator, configurationSupplier, indexName, indexType, bulkSize, timeout);
 JavaPairRDD<String, MyEsData> myEsDataRdd = ...;			
 pipeline.process(esIndexerData);//every partition will be converted into separate shard
  ```
