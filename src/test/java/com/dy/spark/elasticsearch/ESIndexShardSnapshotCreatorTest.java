package com.dy.spark.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHitField;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.google.common.base.Supplier;
import com.google.common.io.Files;

public class ESIndexShardSnapshotCreatorTest implements Serializable {
	private static final String MY_BACKUP_REPO = "my_backup_repo";
	private static final int TIMEOUT = 10000;
	protected transient JavaSparkContext sc;
	protected File tempDir;
	private String indexName;
	private String snapshotRepoName;
	private String esWorkingBaseDir;
	private String snapshotBase;
	private String snapshotLocation;
	private int partitionsNum;
	private int totalNumberOfDocsPerPartition;

	@Before
	public void setUp() throws Exception {
		tempDir = Files.createTempDir();
		tempDir.deleteOnExit();
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[*]");
		sparkConf.setAppName(getClass().getSimpleName());
		sparkConf.set("spark.local.dir", tempDir + "/spark");
		sc = new JavaSparkContext(sparkConf);
		sc.setCheckpointDir(tempDir + "/checkpoint/");
		
		snapshotBase = tempDir + "/es-test/snapshots-work/";
		esWorkingBaseDir = tempDir + "/es-test/es-work/";
		snapshotLocation = tempDir + "/es-test/my_backup_repo/";
	}

	@After
	public void tearDown() {
		if (sc != null) {
			sc.stop();
			sc = null;
		}
		FileUtils.deleteQuietly(tempDir);
	}

	@RequiredArgsConstructor
	@Data
	public static final class MyData implements Serializable {
		final int a;
		final String b;
	}

	static final class ConfigSupplier implements Supplier<Configuration>, Serializable {
		@Override
		public Configuration get() {
			return new Configuration();
		}
	}

	@Test
	public void test() throws Exception {
		String destination = "file://" + snapshotLocation;
		createSnapshot(snapshotBase, esWorkingBaseDir, destination);
		
		restorePreparedSnapshotAndVerify();
	}

	private void restorePreparedSnapshotAndVerify() throws InterruptedException, ExecutionException {
		String esWorkingDir = Joiner.on("/").join(esWorkingBaseDir, indexName);
		String nodeName = Joiner.on("_").join("embededESTempLoaderNode", indexName);
		String clusterName = Joiner.on("_").join("embededESTempLoaderCluster", indexName);

		org.elasticsearch.common.settings.ImmutableSettings.Builder builder = ImmutableSettings
				.builder()
				// Disable HTTP transport, we'll communicate inner-jvm
				.put("http.enabled", false)
				.put("processors", 1)
				.put("node.name", nodeName)
				.put("path.home", esWorkingDir)
				.put("bootstrap.mlockall", true)
				.put("indices.memory.index_buffer_size", "5%")
				.put("path.repo", snapshotLocation)
				// feature to play with when that's out
				.put("indices.fielddata.cache.size", "0%");
		Settings nodeSettings = builder.build();
		Node node = nodeBuilder().client(false).local(true).clusterName(clusterName).settings(nodeSettings).build();

		node.start();
		// Create the snapshot repo
		Map<String, Object> repositorySettings = new HashMap<>();
		repositorySettings.put("location", snapshotLocation);
		repositorySettings.put("compress", true);
		node.client().admin().cluster().preparePutRepository(snapshotRepoName).setType("fs")
				.setSettings(repositorySettings).get();

		String snapshotName = Joiner.on("_").join(ESIndexShardSnapshotCreator.SNAPSHOT_NAME_PREFIX, indexName);
		node.client().admin().cluster().restoreSnapshot(new RestoreSnapshotRequest(snapshotRepoName, snapshotName)).get();
		node.client().admin().cluster().health(new ClusterHealthRequest().waitForYellowStatus().timeout(new TimeValue(10, TimeUnit.SECONDS))).actionGet();
		CountResponse response = node.client().prepareCount(indexName).execute().actionGet();
		assertEquals(totalNumberOfDocsPerPartition * partitionsNum, response.getCount());
		
		
	/*	for (int i = 0; i < totalNumberOfDocsPerPartition * partitionsNum; i++) {
			SearchRequestBuilder searchRequestBuilder = node.client().prepareSearch(indexName).setSearchType(SearchType.QUERY_AND_FETCH);		
			
			searchRequestBuilder.setQuery(QueryBuilders.termQuery("_id", String.valueOf(i)));
			searchRequestBuilder.addField("a");		
			searchRequestBuilder.addField("b");
			System.out.println("es query:" + searchRequestBuilder.toString());
			SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
			Assert.assertEquals(1, searchResponse.getHits().getTotalHits());
			
			Map<String, SearchHitField> fields = searchResponse.getHits().getHits()[0].getFields();

			
			Integer a = fields.get("a").getValue();
			assertEquals(i%totalNumberOfDocsPerPartition, a.intValue());
			String b = fields.get("b").getValue();
			assertEquals(String.valueOf(i), b);
		}*/
	}

	private void createSnapshot(String snapshotBase, String esWorkingBaseDir, String destination)
			throws MalformedURLException, URISyntaxException, IOException, Exception {
		File templateFile = new File(ESIndexShardSnapshotCreatorTest.class.getResource("template.json").toURI().toURL()
				.getFile());
		String templateJson = FileUtils.readFileToString(templateFile);
		Map<String,String> additionalEsSettings = new HashMap<>();
		ESFilesTransport transport = new ESFilesTransport();
		snapshotRepoName = MY_BACKUP_REPO;
		final ESIndexShardSnapshotCreator creator = new ESIndexShardSnapshotCreator(transport, additionalEsSettings, snapshotBase,
				destination, snapshotRepoName, esWorkingBaseDir, "es-template", templateJson, 100, 512);
		indexName = "my-index_" + System.currentTimeMillis();
		final String indexType = "mydata";// should be consistent with
											// template.json
		Supplier<Configuration> configurationSupplier = new ConfigSupplier();
		final int bulkSize = 10000;
		ESIndexShardSnapshotPipeline<String, MyData> pipeline = new ESIndexShardSnapshotPipeline<>(
				creator,
				configurationSupplier, 
				indexName, 
				indexType, 
				bulkSize, 
				TIMEOUT);

		long start = System.currentTimeMillis();
		partitionsNum = 4;
		totalNumberOfDocsPerPartition = 10;

		List<Tuple2<String, MyData>> docs = new ArrayList<>();
		int id = 0;
		for (int part = 0; part < partitionsNum; part++) {
			for (int doc = 0; doc < totalNumberOfDocsPerPartition; doc++) {
				String idAsStr = String.valueOf(id);				
				docs.add(new Tuple2<>(idAsStr, new MyData(doc, idAsStr)));
				id++;
			}
		}
		JavaPairRDD<String, MyData> pairRDD = sc.parallelizePairs(docs).partitionBy(new HashPartitioner(partitionsNum));

		pipeline.process(pairRDD);

		System.out.println("Everything took: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start)
				+ " secs");
		System.out.println("Restore with :\n curl -XPOST '"
				+ String.format("http://localhost:9200/_snapshot/%s/snapshot_%s/_restore", snapshotRepoName, indexName)
				+ "'");
	}

	/*
	 * 0. create /tmp/es-test/my_backup_repo dir and give it all permissions
	 * 1. create on local elasticsearch snapshot repo
	 curl -XPUT 'http://localhost:9200/_snapshot/my_backup_repo' -d '{
	 "type": "fs",
		  "settings": {
		  "location": "/tmp/es-test/my_backup_repo/",
		  "compress": true
	 	}
	 }'
	 */
	public static void main(String[] args) throws Exception {
		String snapshotBase = "/tmp/es-test/snapshots-work/";
		String esWorkingBaseDir = "/tmp/es-test/es-work/";
		String snapshotFinalDestination = "file:///tmp/es-test/my_backup_repo/";

		ESIndexShardSnapshotCreatorTest test = new ESIndexShardSnapshotCreatorTest();
		test.setUp();
		test.createSnapshot(snapshotBase, esWorkingBaseDir, snapshotFinalDestination);
	}

}
