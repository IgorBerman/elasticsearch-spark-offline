# Elasticsearch offline snapshot preparation with spark

## based on [elasticsearch-lambda project](https://github.com/MyPureCloud/elasticsearch-lambda)

## Is it production ready? - Almost, you should test yourself

## How it works
  ```
  Map<String, String> additionalEsSettigns = new HashMap<>();
  additionalEsSettigns.put("es.mapping.exclude", "excludedField");
			
  ESFilesTransport transport = new ESFilesTransport();
  InputStream is = getClass().getResourceAsStream("/es-template.json");
  String templateContent = CharStreams.toString(new InputStreamReader(is));
  final ESIndexShardSnapshotCreator creator = new ESIndexShardSnapshotCreator(
					transport, 
					additionalEsSettigns,
					"/tmp/snapshotsWorkingDir/",
					"s3a://path-of-snapshots",					 
					"snapshot-repo-name", 
					"/tmp/esWorkingDir",  
					"template-name",
					templateContent, 
					10, 
					1024);
 final String indexName = "my-index";//make it different each time
 final String indexType = "type";
 int bulkSize = 10000;
 long timeout = 10000;
 ESIndexShardSnapshotPipeline<String, MyEsData> pipeline = new ESIndexShardSnapshotPipeline<>(creator, configurationSupplier, indexName, indexType, bulkSize, timeout);
 JavaPairRDD<String, MyEsData> myEsDataRdd = ...;			
 pipeline.process(esIndexerData);//every partition will be converted into separate shard

  ```
