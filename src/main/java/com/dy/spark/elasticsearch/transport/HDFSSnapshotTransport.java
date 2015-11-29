package com.dy.spark.elasticsearch.transport;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.ipc.RemoteException;

@Log4j
public class HDFSSnapshotTransport  extends BaseTransport {
	private FileSystem hdfsFileSystem;
	
	@Override
	protected void init() {
	    Configuration conf = new Configuration();
	    try {
			hdfsFileSystem = FileSystem.get(conf);
		} catch (IOException e) {
			throw new IllegalStateException("Unable to initialize HDFSSnapshotTransport because of ", e);
		}
	}

	@Override
	protected void close() {
		
	}
	
	private void ensurePathExists(String destination) throws IOException {
		String[] pieces = StringUtils.split(destination, DIR_SEPARATOR);
		
		String path = "";
		for(String piece : pieces) {
			if(StringUtils.isEmpty(piece)) {
				continue;
			}
			path = path + DIR_SEPARATOR + piece;
			if(!hdfsFileSystem.exists(new Path(path))) {
				try{
					hdfsFileSystem.mkdirs(new Path(path));	
				} catch (IOException e) {
					log.warn("Unable to create path " + path + " likely because it was created in another reducer thread.");
				}
			}
		}
	}

	@Override
	protected void transferFile(boolean deleteSource, String destination, String filename, String localDirectory) throws IOException {
		Path source = new Path(localDirectory + DIR_SEPARATOR + filename);
		ensurePathExists(destination);

		try{
			hdfsFileSystem.copyFromLocalFile(deleteSource, true, source, new Path(destination + DIR_SEPARATOR + filename));	
		}
		catch(LeaseExpiredException | RemoteException e) {
			// This is an expected race condition where 2 reducers are trying to write the manifest files at the same time. That's okay, it only has to succeed once. 
			log.warn("Exception from 2 reducers writing the same file concurrently. One writer failed to obtain a lease. Destination " + destination + " filename " + filename + " localDirectory " + localDirectory, e);
		}
	}

	@Override
	protected void transferDir(String destination, String localShardPath, String shard) throws IOException {
		destination = destination + shard + DIR_SEPARATOR;
		ensurePathExists(destination);
		try{
			File[] files = new File(localShardPath).listFiles();
			for (File file : files) {
				transferFile(true, destination, file.getName(), localShardPath);
			}
		} catch(FileNotFoundException e) {
			throw new FileNotFoundException("Exception copying " + localShardPath + " to " + destination);
		} 
	}

	@Override
	protected boolean checkExists(String destination, Integer shardNumber) throws IOException {
		return hdfsFileSystem.exists(new Path(destination + shardNumber));
	}

	@Override
	protected String name() {
		return "hdfs";
	}
}
