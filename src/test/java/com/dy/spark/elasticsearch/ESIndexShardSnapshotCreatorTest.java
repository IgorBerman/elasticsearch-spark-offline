package com.dy.spark.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.google.common.base.Supplier;
import com.google.common.io.Files;

public class ESIndexShardSnapshotCreatorTest implements Serializable {
	private static final int TIMEOUT = 10000;
	protected transient JavaSparkContext sc;
	protected File tempDir;

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
		String snapshotBase = tempDir + "/es-test/snapshots-work/";
		String esWorkingBaseDir = tempDir + "/es-test/es-work/";
		String destination = "file://" + tempDir + "/es-test/my_backup_repo/";

		createSnapshot(snapshotBase, esWorkingBaseDir, destination);
	}

	private void createSnapshot(String snapshotBase, String esWorkingBaseDir, String destination)
			throws MalformedURLException, URISyntaxException, IOException, Exception {
		File templateFile = new File(ESIndexShardSnapshotCreatorTest.class.getResource("template.json").toURI().toURL()
				.getFile());
		String templateJson = FileUtils.readFileToString(templateFile);

		ESFilesTransport transport = new ESFilesTransport();
		String snapshotRepoName = "my_backup_repo";
		final ESIndexShardSnapshotCreator creator = new ESIndexShardSnapshotCreator(transport, snapshotBase,
				destination, snapshotRepoName, esWorkingBaseDir, "es-template", templateJson, 100, 512);
		final String indexName = "my-index_" + System.currentTimeMillis();
		final String indexType = "mydata";// should be consistent with
											// template.json
		Supplier<Configuration> configurationSupplier = new ConfigSupplier();
		final int bulkSize = 10000;
		ESIndexShardSnapshotPipeline<MyData> pipeline = new ESIndexShardSnapshotPipeline<>(creator,
				configurationSupplier, indexName, indexType, bulkSize, TIMEOUT);

		long start = System.currentTimeMillis();
		final int partitionsNum = 4;
		final int totalNumberOfDocsPerPartition = 10;// 5_000_000;//1_078_671_786;

		List<Tuple2<String, MyData>> docs = new ArrayList<>();
		for (int part = 0; part < partitionsNum; part++) {
			final int partNum = part;
			for (int doc = 0; doc < totalNumberOfDocsPerPartition; doc++) {
				String id = doc + "-" + partNum;
				docs.add(new Tuple2<>(id, new MyData(doc, id)));
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
