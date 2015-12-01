package com.dy.spark.elasticsearch.transport;

import java.io.File;
import java.io.IOException;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@RequiredArgsConstructor
@Log4j
public class ESFilesTransport {
	static final String DIR_SEPARATOR = File.separator;
	private final FileSystem fs;

	/**
	 * Moves prepared locally shards data or index metadata to destination
	 * @param snapshotName
	 * @param index
	 * @param snapshotWorkingLocation - local path
	 * @param snapshotFinalDestination - remote path(hdfs/s3/local)
	 * @param destShard
	 * @param moveShards - either to move shard data or index metadata
	 * @throws IOException
	 */
	public void move(String snapshotName, String index, String snapshotWorkingLocation, String destination, int destShard, boolean moveShards) throws IOException {
		// Figure out which shard has all the data
		String baseIndexShardLocation = snapshotWorkingLocation + DIR_SEPARATOR + "indices" + DIR_SEPARATOR+index;
		
		if (moveShards) {
			String largestShard = getShardSource(baseIndexShardLocation);
			
			// Upload shard data
			String shardSource =baseIndexShardLocation + DIR_SEPARATOR + largestShard;
			
			String shardDestination = destination + DIR_SEPARATOR + "indices" + DIR_SEPARATOR + index + DIR_SEPARATOR;
			transferDir(shardDestination, shardSource, destShard);
		} else {
			// Upload top level manifests
			transferFile(destination, "metadata-" + snapshotName, snapshotWorkingLocation);
			transferFile(destination, "snapshot-" + snapshotName, snapshotWorkingLocation);
			transferFile(destination, "index", snapshotWorkingLocation);
			
			
			// Upload per-index manifests
			String indexManifestSource =  baseIndexShardLocation;
			String indexManifestDestination = destination + DIR_SEPARATOR + "indices" + DIR_SEPARATOR + index;
			transferFile(indexManifestDestination, "snapshot-" + snapshotName, indexManifestSource);
		}
	}
	
	/**
	 * We've snapshotted an index with all data routed to a single shard (1 shard)
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
	
	
	private void ensurePathExists(String destination) throws IOException {
		String[] pieces = StringUtils.split(destination, DIR_SEPARATOR);
		
		String path = "";
		for(String piece : pieces) {
			if(StringUtils.isEmpty(piece)) {
				continue;
			}
			path = path + DIR_SEPARATOR + piece;
			if(!fs.exists(new Path(path))) {
				try{
					fs.mkdirs(new Path(path));	
				} catch (IOException e) {
					log.error("Unable to create path " + path + " likely because it was created in another reducer thread.", e);
					throw e;
				}
			}
		}
	}

	private void transferFile(String destination, String filename, String localDirectory) throws IOException {
		Path source = new Path(localDirectory + DIR_SEPARATOR + filename);
		ensurePathExists(destination);
		fs.copyFromLocalFile(false, true, source, new Path(destination + DIR_SEPARATOR + filename));	
	}

	private void transferDir(String destination, String localShardPath, int shard) throws IOException {
		destination = destination + shard + DIR_SEPARATOR;
		ensurePathExists(destination);
		File[] files = new File(localShardPath).listFiles();
		for (File file : files) {
			transferFile(destination, file.getName(), localShardPath);
		}
	}
}
