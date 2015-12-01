package com.dy.spark.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import com.dy.spark.elasticsearch.transport.ESFilesTransport;

import scala.Tuple2;

public class ESIndexShardSnapshotCreatorTest {
	private static final int TIMEOUT = 10000;

	static class EsShardIndexingTask implements Runnable {
		private final int bulkSize;
		private final String indexType;
		private final int partNum;
		private final ESIndexShardSnapshotCreator creator;
		private final int totalNumberOfDocsPerPartition;
		private final String indexName;

		private EsShardIndexingTask(int bulkSize, String indexType, int partNum,
				ESIndexShardSnapshotCreator creator, int totalNumberOfDocsPerPartition, String indexName) {
			this.bulkSize = bulkSize;
			this.indexType = indexType;
			this.partNum = partNum;
			this.creator = creator;
			this.totalNumberOfDocsPerPartition = totalNumberOfDocsPerPartition;
			this.indexName = indexName;
		}

		@Override
		public void run() {
			List<Tuple2<String, MyData>> docs = new ArrayList<>();
			System.out.println("Preparing");
			for (int doc = 0; doc <  totalNumberOfDocsPerPartition; doc++) {
				String id = doc+"-" +partNum;
				docs.add(new Tuple2<>(id, new MyData(doc, id)));
			}
			try {
				System.out.println("Creating");
				//this will be done concurrently by workers
				creator.<MyData>create(indexName, partNum, bulkSize, "", indexType, docs.iterator(), TIMEOUT);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@RequiredArgsConstructor
	static final class MyData {
		public final int a;
		public final String b;
	}

	/*
	 * To run test:
	 * 0. create /tmp/es-test/my_backup_repo dir and give it all permissions
	 * 1. create on local elasticsearch snapshot repo
	  curl -XPUT 'http://localhost:9200/_snapshot/my_backup_repo' -d '{
			    "type": "fs",
			    "settings": {
			        "location": "/tmp/es-test/my_backup_repo/",
			        "compress": true
			    }
			}'
	 * 2. run test
	 * 3.
	  curl -XPOST 'http://localhost:9200/_snapshot/my_backup_repo/snapshot_my-index_1448807814444/_restore' 
	 * 4. you should see 12 docs...
	*/
	@Test
	public void test() throws IOException, URISyntaxException, InterruptedException {		
		String snapshotBase = "/tmp/es-test/snapshots-work/";
		String snapshotFinalDestination="file:///tmp/es-test/my_backup_repo/";
		String esWorkingBaseDir="/tmp/es-test/es-work/";
		File templateFile = new File(ESIndexShardSnapshotCreatorTest.class.getResource("template.json").toURI().toURL().getFile());
		String templateJson = FileUtils.readFileToString(templateFile);
		
		URI finalDestURI = new File(snapshotFinalDestination).toURI();
		ESFilesTransport transport = new ESFilesTransport(FileSystem.get(finalDestURI, new Configuration()));
		final ESIndexShardSnapshotCreator creator = new ESIndexShardSnapshotCreator(transport, 
				snapshotBase, 
				snapshotFinalDestination,
				"my_backup_repo", 
				esWorkingBaseDir, 
				"es-template", 
				templateJson,
				100,
				512);
		final String indexName = "my-index_" +System.currentTimeMillis();
		final String indexType = "mydata";//should be consistent with template.json
		
		long start = System.currentTimeMillis();
		final int partitionsNum = 4;
		final int bulkSize = 10000;
		final int totalNumberOfDocsPerPartition = 10;//5_000_000;//1_078_671_786;
		ExecutorService pool = Executors.newFixedThreadPool(partitionsNum);
		
		for (int part = 0; part < partitionsNum; part++) {
			final int partNum = part;
			pool.submit(new EsShardIndexingTask(bulkSize, indexType, partNum, creator, totalNumberOfDocsPerPartition, indexName));
		}
		pool.shutdown();
		while(!pool.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS)) {
			System.out.println("Awaiting termination...");
		}
		//this stage will be done by driver(?) or one of the workers
		creator.postprocess(indexName, partitionsNum, "", indexType, TIMEOUT);
		System.out.println("Everything took: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) + " secs");
	}

}
