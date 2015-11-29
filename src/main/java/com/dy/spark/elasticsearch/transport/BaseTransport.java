package com.dy.spark.elasticsearch.transport;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;

public abstract class BaseTransport {
	static final String DIR_SEPARATOR = File.separator;
	protected abstract void init();
	protected abstract void close();
	protected abstract void transferFile(boolean deleteSource, String destination, String filename, String localDirectory) throws IOException;
	protected abstract void transferDir(String destination, String localShardPath, String shard) throws IOException;
	protected abstract boolean checkExists(String destination, Integer shardNumber) throws IOException;
	protected abstract String name();

	/**
	 * Transport a snapshot sitting on the local filesystem to a remote repository. Snapshots are stiched together
	 * shard by shard because we're snapshotting 1 shard at a time. 
	 * 
	 * @param snapshotName
	 * @param index
	 * @param shardNumber
	 * @throws IOException
	 */
	public void move(String snapshotName, String index, String snapshotWorkingLocation, String snapshotFinalDestination, String destShard, boolean moveShards) throws IOException {
		init();
		// Figure out which shard has all the data
		String baseIndexShardLocation = snapshotWorkingLocation + DIR_SEPARATOR + "indices" + DIR_SEPARATOR+index;
		String destination = removeStorageSystemFromPath(snapshotFinalDestination);
		
		if (moveShards) {
			String largestShard = getShardSource(baseIndexShardLocation);
			
			// Upload shard data
			String shardSource =baseIndexShardLocation + DIR_SEPARATOR + largestShard;
			
			String shardDestination = destination + DIR_SEPARATOR + "indices" + DIR_SEPARATOR + index + DIR_SEPARATOR;
			transferDir(shardDestination, shardSource, destShard);
		} else {
			// Upload top level manifests
			transferFile(false, destination, "metadata-" + snapshotName, snapshotWorkingLocation);
			transferFile(false, destination, "snapshot-" + snapshotName, snapshotWorkingLocation);
			transferFile(false, destination, "index", snapshotWorkingLocation);
			
			
			// Upload per-index manifests
			String indexManifestSource =  baseIndexShardLocation;
			String indexManifestDestination = destination + DIR_SEPARATOR + "indices" + DIR_SEPARATOR + index;
			transferFile(false, indexManifestDestination, "snapshot-" + snapshotName, indexManifestSource);
		}
		close();
	}
	
	/**
	 * Rip out filesystem specific stuff off the path EG s3:// 
	 * @param s
	 * @return s
	 */
	private String removeStorageSystemFromPath(String s) {
		return s.replaceFirst(name() + "://", "");			
	}
	
	/**
	 * We've snapshotted an index with all data routed to a single shard (1 shard per reducer). Problem is 
	 * we don't know which shard # it routed all the data to. We can determine that by picking 
	 * out the largest shard folder and renaming it to the shard # we want it to be.
	 */
	private String getShardSource(String baseIndexLocation) throws IOException {
		// Get a list of shards in the snapshot
		
		File file = new File(baseIndexLocation);
		String[] shardDirectories = file.list(DirectoryFileFilter.DIRECTORY);
		
		// Figure out which shard has all the data in it. Since we've routed all data to it, there'll only be one
		Long biggestDirLength = null;
		String biggestDir = null;
		for(String directory : shardDirectories) {
			File curDir = new File(baseIndexLocation +DIR_SEPARATOR+ directory);
            long curDirLength = FileUtils.sizeOfDirectory(curDir);
			if(biggestDirLength == null || biggestDirLength < curDirLength) {
				biggestDir = directory;
				biggestDirLength = curDirLength;
			}
		}
		
		return biggestDir;
	}
}
