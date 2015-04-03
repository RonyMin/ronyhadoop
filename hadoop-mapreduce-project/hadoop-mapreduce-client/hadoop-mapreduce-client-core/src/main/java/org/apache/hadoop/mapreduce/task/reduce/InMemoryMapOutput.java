/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class InMemoryMapOutput<K, V> extends MapOutput<K, V> {
  private static final Log LOG = LogFactory.getLog(InMemoryMapOutput.class);
  private Configuration conf;
  private final MergeManagerImpl<K, V> merger;
  private byte[] memory;
  private BoundedByteArrayOutputStream byteStream;
  // Decompression of map-outputs
  private final CompressionCodec codec;
  private final Decompressor decompressor;
  
  private Set<String> replicationTaskSet;
  private Set<String> existedMOFinLocalReplicationStore;
  private String mapId;
  private FileSystem rawLocalFS;
  private OutputStream disk; 
  private Configuration job = null;
  private Path replicationMOFPath;
  private FSDataInputStream fsdis;
  private boolean isMOFMaterialized;

  public InMemoryMapOutput(Configuration conf, TaskAttemptID mapId,
                           MergeManagerImpl<K, V> merger,
                           int size, CompressionCodec codec,
                           boolean primaryMapOutput) {
	
    super(mapId, (long)size, primaryMapOutput);
    this.conf = conf;
    this.merger = merger;
    this.codec = codec;
    byteStream = new BoundedByteArrayOutputStream(size);
    memory = byteStream.getBuffer();
    if (codec != null) {
      decompressor = CodecPool.getDecompressor(codec);
    } else {
      decompressor = null;
    }
    
    this.mapId = getMapId().toString();
    this.job = new Configuration();
    this.replicationTaskSet = new HashSet<String>();
    this.existedMOFinLocalReplicationStore = new HashSet<String>();
    if(conf.get("rony.network").equals("1G"))
    	job.set("fs.defaultFS", "hdfs://10.150.20.22:8020");
    else
    	job.set("fs.defaultFS", "hdfs://172.30.1.100:8020");
    this.replicationMOFPath = new Path("/data/replication/"+mapId.toString()+"/file.out");
    try {
		this.rawLocalFS = ((LocalFileSystem)FileSystem.getLocal(job)).getRaw();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	this.isMOFMaterialized = false;
    updateExistMOFinLocalReplicationStore();
    updateReplicationTasks();
  }

  public byte[] getMemory() {
    return memory;
  }

  public BoundedByteArrayOutputStream getArrayStream() {
    return byteStream;
  }

  // The main difference between OnDiskMapOutput and InMemoryMapOutput
  // is whether it needs to materialize replication MOF into local replication store or not
  // In case of InMemoryMapOutput with replication task, it materialize replication MOF first.
  // So, we explicitly materialize replication MOF before reading MOF into memory buffer.
  @Override
  public void shuffle(MapHost host, InputStream input,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
	  
	updateReplicationTasks();
	updateExistMOFinLocalReplicationStore();
	
    LOG.info("Deciding replication status of incoming mapID: " + mapId);
    
    // If replication map task is not materialized in this node,
    // we first need to copy that MOF into local replication store
    if(this.replicationTaskSet.contains(mapId)) {

        //long MOFSize = Long.parseLong(hdfs.listStatus(new Path("/replication/"+mapId))[0].getPath().toString().split("/")[5]);
    	long MOFSize = 0;
    	
    	if(!rawLocalFS.exists(replicationMOFPath) || rawLocalFS.getLength(replicationMOFPath) == 0) {
    	
    		disk = rawLocalFS.create(replicationMOFPath);
    		materializeMOFIntoLocalReplicationStore(compressedLength, input, metrics, reporter);
    		MOFSize = rawLocalFS.getLength(replicationMOFPath);
    		LOG.info("Materializing replication MOF finished...! MOFSize: " + MOFSize 
    				+ ", comp Size: " + compressedLength + ", file size: " + rawLocalFS.getFileStatus(replicationMOFPath).getLen());
    		
    	} else if(rawLocalFS.getLength(replicationMOFPath) < compressedLength) {

        	LOG.info("MOFFile exists in local replication store but we may need to wait until finishing the materialization of this MOFFile...");
        	while(rawLocalFS.getLength(replicationMOFPath) < compressedLength) {
        		try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
        	}
        	LOG.info("map ID " + mapId + "'s MOF found in local replication store! --> size: " + rawLocalFS.getLength(replicationMOFPath));

    		/*
    		LOG.info("Target MOF is under the construction. Trying to process another MOF!");
    		throw new java.lang.InternalError();
    		*/
    	}
        
        // Are map-outputs compressed?
        if (codec != null) {
          decompressor.reset();
          input = codec.createInputStream(input, decompressor);
        }
    	
        try {
    		LOG.info("Reading MOF into memory file!");
        	File targetFile = new File(replicationMOFPath.toString());
        	FSDataInputStream fsdis = rawLocalFS.open(replicationMOFPath);
        	IFileInputStream ifis = new IFileInputStream(fsdis, compressedLength, conf);
        	
        	IOUtils.readFully(ifis, memory, 0, memory.length);
        	metrics.inputBytes(memory.length);
        	reporter.progress();
        	LOG.info("Read " + memory.length + " bytes from replication MOF " + getMapId());
        	LOG.info("InMemoryShuffling is finished! current position of FSDIS: " + fsdis.getPos() + ", decompLength: " + decompressedLength);

        	ifis.close();
        	return;

        } catch (IOException ioe) {      
        	// Close the streams
        	IOUtils.cleanup(LOG, input);
        	// Re-throw
        	throw ioe;
        	
        } finally {
        	CodecPool.returnDecompressor(decompressor);
        }
		
    } else {

    	try {
    		
        	IFileInputStream ifis = new IFileInputStream(input, compressedLength, conf);
        	input = ifis;
    		LOG.info("Normal shuffling!");
    		// Normal shuffling (since task is not replication task)
    		IOUtils.readFully(input, memory, 0, memory.length);
    		metrics.inputBytes(memory.length);
    		reporter.progress();
    		LOG.info("Read " + memory.length + " bytes from normal MOF for " +
                getMapId());

    		/**
    		 * We've gotten the amount of data we were expecting. Verify the
    		 * decompressor has nothing more to offer. This action also forces the
    		 * decompressor to read any trailing bytes that weren't critical
    		 * for decompression, which is necessary to keep the stream
    		 * in sync.
    		 */
    		if (input.read() >= 0 ) {
    			throw new IOException("[Normal case] Unexpected extra bytes from input stream for " +
                               getMapId());
    		}

    	} catch (IOException ioe) {      
    		// Close the streams
    		IOUtils.cleanup(LOG, input);

    		// Re-throw
    		throw ioe;
    	} finally {
    		CodecPool.returnDecompressor(decompressor);
    	}
    }
  }

  @Override
  public void commit() throws IOException {
    merger.closeInMemoryFile(this);
  }
  
  @Override
  public void abort() {
    merger.unreserve(memory.length);
  }

  @Override
  public String getDescription() {
    return "MEMORY";
  }

 
  // Rony
  public void updateExistMOFinLocalReplicationStore() {
	  
	  FileSystem localFS = null;
	  Path replicationStorePath;
	  FileStatus[] replicationTaskStatus = null;
	  
	  try {
		localFS = FileSystem.getLocal(job);
	  } catch (IOException e) {
		e.printStackTrace();
	  }
	  
	  replicationStorePath = new Path("/data/replication");
	  LOG.info("ShuffleHandler: updateExistMOFinLocalReplicationStore...");
	  try {
		replicationTaskStatus = localFS.listStatus(replicationStorePath);
	  } catch (Exception e) {
		e.printStackTrace();
	  }
	
	  if(replicationTaskStatus != null) {
		  for(FileStatus fs : replicationTaskStatus) {
			  String task = fs.getPath().toString().split("/")[3];
			  if(!this.existedMOFinLocalReplicationStore.contains(task)) {
				  this.existedMOFinLocalReplicationStore.add(task);
			  }
		  }
	  }
  }
  
  public void updateReplicationTasks() {
	  
	  FileSystem hdfs = null;
	  Path replicationStorePath;
	  FileStatus[] replicationTaskStatus = null;
	  
	  try {
		  hdfs = FileSystem.get(job);
	  } catch (IOException e) {
		e.printStackTrace();
	  }
	  
	  replicationStorePath = new Path("/replication");
	  LOG.info("InMemoryMapOutput: updateReplicationTasks...");
	  try {
		replicationTaskStatus = hdfs.listStatus(replicationStorePath);
	  } catch (Exception e) {
		e.printStackTrace();
	  }
	
	  if(replicationTaskStatus != null) {
		  for(FileStatus fs : replicationTaskStatus) {
			  String task = fs.getPath().toString().split("/")[4];
			  if(!this.replicationTaskSet.contains(task)) {
				  this.replicationTaskSet.add(task);
			  }
		  }
	  }
  }
  
  // Rony
  public boolean isFileAlreadyOpened(Path target) {
	  
	  boolean result = false;
	  File targetFile = new File(target.toString());
	  try {
		  // If target file exists, touch method would be failed
		  // Else, it creates size 0 files in a targetFile path
		  org.apache.commons.io.FileUtils.touch(targetFile);
		
	  } catch (IOException e) {
		result = true;
	  }
	  return result;
  }
  
  public void materializeMOFIntoLocalReplicationStore(long compressedLength, 
		  					InputStream input, ShuffleClientMetrics metrics,
		  					Reporter reporter) throws IOException {

  		IFileInputStream ifis = new IFileInputStream(input, compressedLength, conf);
	  	// Copy data to local-disk
		long bytesLeft = compressedLength;
		
		try {
			LOG.info("Trying to materialize MOF into local replication store...");
			final int BYTES_TO_READ = 64 * 1024;
			byte[] buf = new byte[BYTES_TO_READ];
			while (bytesLeft > 0) {
				int n = ((IFileInputStream)ifis).readWithChecksum(buf, 0, (int) Math.min(bytesLeft, BYTES_TO_READ));
				if (n < 0) {
					throw new IOException("read past end of stream reading " + 
	                               getMapId());
				}
				disk.write(buf, 0, n);
				bytesLeft -= n;
				metrics.inputBytes(n);
				reporter.progress();
			}

			LOG.info("MOF materialization: read " + (compressedLength - bytesLeft) + 
					" bytes from map-output for " + getMapId());
			disk.close();

		} catch (IOException ioe) {
			// Close the streams
			IOUtils.cleanup(LOG, input, disk);

			// Re-throw
			throw ioe;
		}
		// Sanity check
		if (bytesLeft != 0) {
			throw new IOException("Incomplete map output received for " +
	                         getMapId() + " : (" + 
	                         bytesLeft + " bytes missing of " + 
	                         compressedLength + ")");
		}
  }
}
