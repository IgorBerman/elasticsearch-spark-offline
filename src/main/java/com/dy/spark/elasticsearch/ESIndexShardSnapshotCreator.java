package com.dy.spark.elasticsearch;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.PluginsService;

import scala.Tuple2;

import com.dy.spark.elasticsearch.transport.BaseTransport;
import com.google.gson.Gson;

@Log4j
@RequiredArgsConstructor
public class ESIndexShardSnapshotCreator {
	private static final int WAIT_FOR_COMPLETION_DELAY = 1000;
	public static final String SNAPSHOT_NAME_PREFIX = "snapshot";
	private final BaseTransport transport;
	private final String snapshotWorkingLocationBase;
	private final String snapshotFinalDestination;
	private final String snapshotRepoName;
	private final String esWorkingBaseDir;
	private final String templateName;
	private final String templateJson;
	private final int maxMergedSegment;

	public <T> void create(String indexName, int partition, int bulkSize, String routing, String indexType, Iterator<Tuple2<String, T>> docs, long timeout) throws IOException {
		createSnapshotAndMoveToDest(indexName, partition, 1, bulkSize, routing, indexType, docs, timeout, true);
	}
	
	private <T> void createSnapshotAndMoveToDest(String indexName, 
			int partition, 
			int numShardsPerIndex,
			int bulkSize,
			String routing, 
			String indexType, 
			Iterator<Tuple2<String, T>> docs, 
			long timeout, 
			boolean moveShards) throws IOException {
		log.info("Creating snapshot of shard for index " + indexName + "[" + partition +"]");
		String snapshotWorkingLocation = Joiner.on("/").join(snapshotWorkingLocationBase, snapshotRepoName, indexName, partition);
		log.debug("snapshotWorkingLocation " + snapshotWorkingLocation);
		String esWorkingDir = Joiner.on("/").join(esWorkingBaseDir, indexName, partition);
		log.debug("esWorkingDir " + esWorkingDir);
		String nodeName = Joiner.on("_").join("embededESTempLoaderNode", indexName, partition);
		String clusterName = Joiner.on("_").join("embededESTempLoaderCluster", indexName, partition);
		log.debug("node/cluster " + nodeName + "/"+clusterName);
		
		
		org.elasticsearch.common.settings.ImmutableSettings.Builder builder = ImmutableSettings
				.builder()
				// Disable HTTP transport, we'll communicate inner-jvm
				.put("http.enabled", false)
				.put("processors", 1)
				.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShardsPerIndex)
				.put("node.name", nodeName)
				.put("path.home", esWorkingDir)
				// Allow plugins if they're bundled in with the uuberjar
				.put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
				.put("index.refresh_interval", -1)
				// Aggressive flushing helps keep the memory footprint
				.put("index.translog.flush_threshold_size", "128mb")
				.put("bootstrap.mlockall", true)
				// Nodes don't form a cluster, so routing allocations don't
				// matter
				.put("cluster.routing.allocation.disk.watermark.low", 99)
				.put("cluster.routing.allocation.disk.watermark.high", 99)
				.put("index.load_fixed_bitset_filters_eagerly", false)
				// Allow indexing to max out disk IO
				.put("indices.store.throttle.type", "none")
				// The default 10% is a bit large b/c it's calculated against
				// JVM heap size & not Yarn container allocation. Choosing a
				// good value here could be made smarter.
				.put("indices.memory.index_buffer_size", "5%")
				// The default 5gb segment max size is too large for the typical
				// hadoop node
				// .put("index.merge.policy.max_merge_at_once", 10)
				.put("index.merge.policy.max_merged_segment", maxMergedSegment + "mb")
				.put("index.merge.policy.segments_per_tier", 4)
				.put("index.merge.scheduler.max_thread_count", 1)
				.put("path.repo", snapshotWorkingLocation)
				.put("index.compound_format", false) 
				// .put("index.codec", "best_compression") // Lucene 5/ES 2.0
				// feature to play with when that's out
				.put("indices.fielddata.cache.size", "0%");

		Settings nodeSettings = builder.build();

		// Create the node
		Node node = nodeBuilder().client(false) // It's a client + data node
				.local(true) // Tell ES cluster discovery to be inner-jvm only,
								// disable HTTP based node discovery
				.clusterName(clusterName).settings(nodeSettings).build();

		try {
			// Start ES
			node.start();

			node.client().admin().indices().preparePutTemplate(templateName).setSource(templateJson).get();

			// Create the snapshot repo
			Map<String, Object> settings = new HashMap<>();
			settings.put("location", snapshotWorkingLocation);
			settings.put("compress", true);
			settings.put("max_snapshot_bytes_per_sec", "400mb"); // The default
																	// 20mb/sec is
																	// very slow for
																	// a local disk
																	// to disk
																	// snapshot
			node.client().admin().cluster().preparePutRepository(snapshotRepoName).setType("fs").setSettings(settings).get();
			
			log.debug("Creating index " + indexName + "[" + partition +"]" + " with 0 replicas and " + numShardsPerIndex + " number of shards");
			node.client().admin().indices().prepareCreate(indexName)
					.setSettings(settingsBuilder()
							.put("index.number_of_replicas", 0)
							.put("index.number_of_shards", numShardsPerIndex)).get();

			log.info("Starting indexing documents "+ indexName + "[" + partition +"]");
			BulkRequestBuilder bulkRequest = node.client().prepareBulk();
			Gson gson = new Gson();
			
			int countInBulk = 0;
			while (docs.hasNext()) {
				Tuple2<String, T> doc = docs.next();

				IndexRequestBuilder indexRequestBuilder = node
						.client()
						.prepareIndex(indexName, indexType)
						.setId(doc._1())
				// 		.setRouting(routing)
						.setSource(gson.toJson(doc._2()));
				bulkRequest.add(indexRequestBuilder);
				countInBulk ++;
				
				if (countInBulk == bulkSize) {
					log.debug("bulking...");
					submitBulk(bulkRequest);
					countInBulk = 0;
					bulkRequest = node.client().prepareBulk();
				}
			}
			if (countInBulk != 0) {
				submitBulk(bulkRequest);
			}

			TimeValue v = new TimeValue(timeout);

			log.info("Flushing "+indexName+ "[" + partition +"]");
			node.client().admin().indices().prepareFlush(indexName).get(v);

			log.info("Optimizing "+indexName+ "[" + partition +"]");
			node.client().admin().indices().prepareOptimize(indexName).get(v);

			String snapshotName = Joiner.on("_").join(SNAPSHOT_NAME_PREFIX, indexName);
			log.info("Snapshoting "+indexName + "[" + partition +"]" + " as " + snapshotName + " to snapshot repo " + snapshotRepoName);
			node.client().admin().cluster().prepareCreateSnapshot(snapshotRepoName, snapshotName)
					.setWaitForCompletion(true).setIndices(indexName).get();

			log.info("Deleting " + indexName+ "[" + partition +"]");
			ActionFuture<DeleteIndexResponse> response = node.client().admin().indices()
					.delete(new DeleteIndexRequest(indexName));
			while (!response.isDone()) {
				waitForCompletion();
			}

			log.info("Moving shard snapshot of " + indexName+ "[" + partition +"]" + " to destination " + snapshotFinalDestination);
			transport.move(snapshotName, indexName, snapshotWorkingLocation, snapshotFinalDestination, String.valueOf(partition), moveShards);

			log.info("Deleting snapshot of " + indexName+ "[" + partition +"]" + snapshotName);
			node.client().admin().cluster().prepareDeleteSnapshot(snapshotRepoName, snapshotName).execute().actionGet();

			node.close();
			while (!node.isClosed()) {
				waitForCompletion();
			}
		} finally {
			log.info("Cleanup " + snapshotWorkingLocation);
			FileUtils.deleteQuietly(new File(snapshotWorkingLocation));
			log.info("Cleanup " + esWorkingDir);
			FileUtils.deleteQuietly(new File(esWorkingDir));
		}
	}

	private void submitBulk(BulkRequestBuilder bulkRequest) {
		long start = System.currentTimeMillis();
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			for (BulkItemResponse resp : bulkResponse.getItems()) {
				if (resp.getFailure() != null) {
					log.error(resp.getFailureMessage());
				}
			}
		}
		log.info("bulk took " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) + " secs");
	}

	private void waitForCompletion() {
		try {
			Thread.sleep(WAIT_FOR_COMPLETION_DELAY);
		} catch (InterruptedException e) {
			throw new RuntimeException("interrupted", e);
		}
	}

	/**
	 * We create another index snapshot for number of shards
	 */
	public void postprocess(String indexName, int numShardsPerIndex, String routing, String indexType, int timeout) throws IOException {
		Iterator<Tuple2<String, Object>> docs = new ArrayList<Tuple2<String, Object>>().iterator();
		createSnapshotAndMoveToDest(indexName, numShardsPerIndex, numShardsPerIndex, 0, routing, indexType, docs, timeout, false);
	}
}
