package com.dy.spark.elasticsearch;

import java.net.URI;
import java.util.Iterator;

import lombok.RequiredArgsConstructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import com.google.common.base.Supplier;

@RequiredArgsConstructor
public class ESIndexShardSnapshotPipeline<K,V> {
	private final ESIndexShardSnapshotCreator creator;
	private final Supplier<Configuration> configurationSupplier;
	private final String indexName;
	private final String indexType;
	private final int bulkSize;
	private final long timeout;

	public void process(JavaPairRDD<K, V> pairRDD) throws Exception {
		URI finalDestURI = new URI(creator.getSnapshotDestination());
		Function2<Integer, Iterator<Tuple2<K, V>>, Iterator<Void>> snapshotFunc = 
			new ESIndexShardSnapshotFunction<K,V>(creator, configurationSupplier, creator.getSnapshotDestination(), indexName, bulkSize, indexType, timeout);
		
		pairRDD.mapPartitionsWithIndex(snapshotFunc, true).count();
		
		FileSystem fs = FileSystem.get(finalDestURI, configurationSupplier.get());		
		creator.postprocess(fs, indexName, pairRDD.partitions().size(), "", indexType, timeout);
	}
}