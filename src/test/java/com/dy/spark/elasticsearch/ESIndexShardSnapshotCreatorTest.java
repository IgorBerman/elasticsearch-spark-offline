package com.dy.spark.elasticsearch;

import java.io.File;
import java.io.Serializable;
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
			sparkConf.set("spark.local.dir", tempDir+"/spark");
			sc = new JavaSparkContext(sparkConf);
			sc.setCheckpointDir(tempDir+"/checkpoint/");
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
	static final class MyData implements Serializable {
		public final int a;
		public final String b;
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
		String esWorkingBaseDir=tempDir + "/es-test/es-work/";
		File templateFile = new File(ESIndexShardSnapshotCreatorTest.class.getResource("template.json").toURI().toURL().getFile());
		String templateJson = FileUtils.readFileToString(templateFile);
		
		String destination = "file://" + tempDir + "/es-test/my_backup_repo/";
		
		ESFilesTransport transport = new ESFilesTransport();
		final ESIndexShardSnapshotCreator creator = new ESIndexShardSnapshotCreator(transport, 
				snapshotBase, 
				destination,
				"my_backup_repo", 
				esWorkingBaseDir, 
				"es-template", 
				templateJson,
				100,
				512);
		final String indexName = "my-index_" +System.currentTimeMillis();
		final String indexType = "mydata";//should be consistent with template.json
		Supplier<Configuration> configurationSupplier = new ConfigSupplier();
		final int bulkSize = 10000;
		ESIndexShardSnapshotPipeline<MyData> pipeline = new ESIndexShardSnapshotPipeline<>(
				creator, 
				configurationSupplier, 
				indexName,
				indexType, 
				bulkSize, 
				TIMEOUT);
		
		long start = System.currentTimeMillis();
		final int partitionsNum = 4;
		final int totalNumberOfDocsPerPartition = 10;//5_000_000;//1_078_671_786;
		
		List<Tuple2<String, MyData>> docs = new ArrayList<>();
		for (int part = 0; part < partitionsNum; part++) {
			final int partNum = part;
			for (int doc = 0; doc <  totalNumberOfDocsPerPartition; doc++) {
				String id = doc+"-" +partNum;
				docs.add(new Tuple2<>(id, new MyData(doc, id)));
			}
		}
		JavaPairRDD<String,MyData> pairRDD = sc.parallelizePairs(docs).partitionBy(new HashPartitioner(partitionsNum));		
		
		pipeline.process(pairRDD);
		
		
		System.out.println("Everything took: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) + " secs");
	}

}
