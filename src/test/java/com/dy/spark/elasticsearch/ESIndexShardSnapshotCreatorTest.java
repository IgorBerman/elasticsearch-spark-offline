package com.dy.spark.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

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
	public void test() throws IOException, URISyntaxException {
		String snapshotBase = "/tmp/es-test/snapshots-work/";
		String snapshotFinalDestination="/tmp/es-test/my_backup_repo/";
		BaseTransport transport = new LocalFSSnapshotTransport();
		String esWorkingBaseDir="/tmp/es-test/es-work/";
		File templateFile = new File(ESIndexShardSnapshotCreatorTest.class.getResource("template.json").toURI().toURL().getFile());
		String templateJson = FileUtils.readFileToString(templateFile);
		
		ESIndexShardSnapshotCreator creator = new ESIndexShardSnapshotCreator(transport, 
				snapshotBase, 
				snapshotFinalDestination,
				"my_backup_repo", 
				esWorkingBaseDir, 
				"es-template", 
				templateJson,
				100);
		String indexName = "my-index_" +System.currentTimeMillis();
		String indexType = "mydata";//should be consistent with template.json
		
		int partitionsNum = 4;
		//this will be done concurrently by workers
		for (int part = 0; part < partitionsNum; part++) {
			List<Tuple2<String, MyData>> docs = new ArrayList<>();
			for (int doc = 1; doc < 4; doc++) {
				String id = doc+"-" +part;
				docs.add(new Tuple2<>(id, new MyData(doc, id)));
			}
			creator.<MyData>create(indexName, part, "", indexType, docs.iterator(), TIMEOUT);
		}
		//this stage will be done by driver(?) or one of the workers
		creator.postprocess(indexName, partitionsNum, "", indexType, TIMEOUT);
	}

}
