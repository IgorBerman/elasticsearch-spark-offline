package com.dy.spark.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import scala.Tuple2;

import com.dy.spark.elasticsearch.transport.BaseTransport;
import com.dy.spark.elasticsearch.transport.LocalFSSnapshotTransport;

public class ESIndexShardSnapshotCreatorTest {
	private static final int TIMEOUT = 1000000;

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
		String snapshotFinalDestination="/tmp/es-test/my_backup_repo/";
		BaseTransport transport = new LocalFSSnapshotTransport();
		String esWorkingBaseDir="/tmp/es-test/es-work/";
		File templateFile = new File(ESIndexShardSnapshotCreatorTest.class.getResource("template.json").toURI().toURL().getFile());
		String templateJson = FileUtils.readFileToString(templateFile);
		
		final ESIndexShardSnapshotCreator creator = new ESIndexShardSnapshotCreator(transport, 
				snapshotBase, 
				snapshotFinalDestination,
				"my_backup_repo", 
				esWorkingBaseDir, 
				"es-template", 
				templateJson,
				100);
		final String indexName = "my-index_" +System.currentTimeMillis();
		final String indexType = "mydata";//should be consistent with template.json
		
		long start = System.currentTimeMillis();
		final int partitionsNum = 4;
		final int bulkSize = 10000;
		final int totalNumberOfDocs = 1_000_000;//1_078_671_786;
		ExecutorService pool = Executors.newFixedThreadPool(partitionsNum);
		//this will be done concurrently by workers
		for (int part = 0; part < partitionsNum; part++) {
			final int partNum = part;
			pool.submit(new Runnable() {
				@Override
				public void run() {
					List<Tuple2<String, MyData>> docs = new ArrayList<>();
					for (int doc = 0; doc <  totalNumberOfDocs/partitionsNum; doc++) {
						String id = doc+"-" +partNum;
						docs.add(new Tuple2<>(id, new MyData(doc, id)));
					}
					try {
						creator.<MyData>create(indexName, partNum, bulkSize, "", indexType, docs.iterator(), TIMEOUT);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			});
		}
		while(!pool.isTerminated()) {
			System.out.println("Awaiting termination...");
			pool.awaitTermination(TIMEOUT, TimeUnit.SECONDS);
		}
		//this stage will be done by driver(?) or one of the workers
		creator.postprocess(indexName, partitionsNum, "", indexType, TIMEOUT);
		System.out.println("Everything took: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) + " secs");
	}

}
