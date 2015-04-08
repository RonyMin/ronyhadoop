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
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl.CompressAwarePath;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class OnDiskMapOutput<K, V> extends MapOutput<K, V> {
  private static final Log LOG = LogFactory.getLog(OnDiskMapOutput.class);
  private final FileSystem fs;
  private Path tmpOutputPath;
  private final Path outputPath;
  private final MergeManagerImpl<K, V> merger;
  private OutputStream disk; 
  private long compressedSize;
  
  private Set<String> replicationTaskSet;
  private Set<String> existedMOFinLocalReplicationStore;
  private String mapId;
  private FileSystem rawLocalFS;
  private Configuration conf;
  private Path replicationMOFPath;
  private IFileInputStream ifis;
  private FSDataInputStream fsdis;
  private boolean isMOFMaterialized;

  public OnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
                         MergeManagerImpl<K,V> merger, long size,
                         JobConf conf,
                         MapOutputFile mapOutputFile,
                         int fetcher, boolean primaryMapOutput)
      throws IOException {
    this(mapId, reduceId, merger, size, conf, mapOutputFile, fetcher,
        primaryMapOutput, FileSystem.getLocal(conf),
        mapOutputFile.getInputFileForWrite(mapId.getTaskID(), size));
  }

  @VisibleForTesting
  OnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
                         MergeManagerImpl<K,V> merger, long size,
                         JobConf conf,
                         MapOutputFile mapOutputFile,
                         int fetcher, boolean primaryMapOutput,
                         FileSystem fs, Path outputPath) throws IOException {
    super(mapId, size, primaryMapOutput);
    this.fs = fs;
    this.merger = merger;
    this.outputPath = outputPath;
    tmpOutputPath = getTempPath(outputPath, fetcher);
    this.conf = conf;
    this.mapId = getMapId().toString();
    this.replicationMOFPath = new Path("/data/replication/"+this.mapId+"/file.out");
    try {
		this.rawLocalFS = ((LocalFileSystem)FileSystem.getLocal(this.conf)).getRaw();
	} catch (IOException e) {
		e.printStackTrace();
	}
    this.replicationTaskSet = new HashSet<String>();
    this.existedMOFinLocalReplicationStore = new HashSet<String>();
	this.isMOFMaterialized = false;

    updateExistMOFinLocalReplicationStore();
    updateReplicationTasks();
    
  }

  @VisibleForTesting
  static Path getTempPath(Path outPath, int fetcher) {
    return outPath.suffix(String.valueOf(fetcher));
  }

  @Override
  public void shuffle(MapHost host, InputStream input,long compressedLength, 
		  			  long decompressedLength, ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
	  
	    updateExistMOFinLocalReplicationStore();
	    updateReplicationTasks();
	  
	    input = new IFileInputStream(input, compressedLength, conf);
	    LOG.info("Deciding replication status of incoming mapID: " + mapId);
	    
	    // If replication map task is not materialized in this node,
	    // we first need to copy that MOF into local replication store
	    if(this.replicationTaskSet.contains(mapId)) {
	    	
	    	LOG.info("Replication MOF case..");

	    	long MOFSize = 0;
	    	// If MOF is not materialized..
	    	if(!rawLocalFS.exists(replicationMOFPath) || rawLocalFS.getLength(replicationMOFPath) == 0) {
	    		
	    		disk = rawLocalFS.create(replicationMOFPath);
	    		materializeMOFIntoLocalReplicationStore(compressedLength, input, metrics, reporter);
	    		MOFSize = rawLocalFS.getLength(replicationMOFPath);
	    		LOG.info("Materializing replication MOF finished...! MOFSize: " + MOFSize 
	    				+ ", comp Size: " + compressedLength + ", file size: " + rawLocalFS.getFileStatus(replicationMOFPath).getLen());
	  		  	disk.close();
	  		  	
	    	// In the middle of MOF materialization
	    	} else if(rawLocalFS.getLength(replicationMOFPath) < compressedLength) {
	    		
	        	LOG.info("MOFFile exists in local replication store but we may need to wait until finishing the materialization of this MOFFile...");
	        	while(rawLocalFS.getLength(replicationMOFPath) < compressedLength) {
	        		try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
	        	}
	        	LOG.info("map ID " + mapId + "'s MOF is finally stored at the local replication store! --> size: " + rawLocalFS.getLength(replicationMOFPath));
	    		/*
	    		LOG.info("Target MOF is under the construction. Trying to process another MOF!");
	    		throw new java.lang.InternalError();
	    		*/
	    	}
		
		// MOF is (already) materialized into local replication store
	    // Try to read required MOF from local replication store
    	LOG.info("Reading MOF into memory file!");
		long bytesLeft = compressedLength;
		try {
			
	        disk = fs.create(tmpOutputPath);
        	File targetFile = new File(replicationMOFPath.toString());
        	FSDataInputStream fsdis = rawLocalFS.open(replicationMOFPath);
        	IFileInputStream ifis = new IFileInputStream(fsdis, compressedLength, conf);
			
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

			LOG.info("[Replicated MOF] Read " + (compressedLength - bytesLeft) + 
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
                           getMapId() + ": (" + 
                           bytesLeft + " bytes missing of " + 
                           compressedLength + ")");
		}
		
	} else {
		
    	LOG.info("Normal MOF case..");
    	
        disk = fs.create(tmpOutputPath);
		// Normal shuffling
	    long bytesLeft = compressedLength;
	    try {
	      final int BYTES_TO_READ = 64 * 1024;
	      byte[] buf = new byte[BYTES_TO_READ];
	      while (bytesLeft > 0) {
	        int n = ((IFileInputStream)input).readWithChecksum(buf, 0, (int) Math.min(bytesLeft, BYTES_TO_READ));
	        if (n < 0) {
	          throw new IOException("read past end of stream reading " + 
	                                getMapId());
	        }
	        disk.write(buf, 0, n);
	        bytesLeft -= n;
	        metrics.inputBytes(n);
	        reporter.progress();
	      }

	      LOG.info("[Normal MOF] Read " + (compressedLength - bytesLeft) + 
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
	                            getMapId() + " from " +
	                            host.getHostName() + " (" + 
	                            bytesLeft + " bytes missing of " + 
	                            compressedLength + ")");
	    }
	    this.compressedSize = compressedLength;
	}
  }

  @Override
  public void commit() throws IOException {
    fs.rename(tmpOutputPath, outputPath);
    CompressAwarePath compressAwarePath = new CompressAwarePath(outputPath,
        getSize(), this.compressedSize);
    merger.closeOnDiskFile(compressAwarePath);
  }
  
  @Override
  public void abort() {
    try {
      fs.delete(tmpOutputPath, false);
    } catch (IOException ie) {
      LOG.info("failure to clean up " + tmpOutputPath, ie);
    }
  }

  @Override
  public String getDescription() {
    return "DISK";
  }
  
  
  // Rony
  public void updateExistMOFinLocalReplicationStore() {
	  
	  FileSystem localFS = null;
	  Path replicationStorePath;
	  FileStatus[] replicationTaskStatus = null;
	  
	  try {
		localFS = FileSystem.getLocal(conf);
	  } catch (IOException e) {
		e.printStackTrace();
	  }
	  
	  replicationStorePath = new Path("/data/replication");
	  LOG.info("ShuffleHandler: updateReplicationTasks...");
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
		  hdfs = FileSystem.get(conf);
	  } catch (IOException e) {
		e.printStackTrace();
	  }
	  
	  replicationStorePath = new Path("/replication");
	  LOG.info("OnDiskMapOutput: updateReplicationTasks...");
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
  public boolean isFileOpened(Path target) {
	  
	  boolean result = false;
	  File targetFile = new File(target.toString());
	  try {
		org.apache.commons.io.FileUtils.touch(targetFile);
	} catch (IOException e) {
		result = true;
	}
	  return result;
  }
  
  public void materializeMOFIntoLocalReplicationStore(long compressedLength, 
			InputStream input, ShuffleClientMetrics metrics,
			Reporter reporter) throws IOException {
  
	  // Copy data to local-disk
	  long bytesLeft = compressedLength;

	  try {
		  LOG.info("Trying to materialize MOF into local replication store...");
		  final int BYTES_TO_READ = 64 * 1024;
		  byte[] buf = new byte[BYTES_TO_READ];
		  while (bytesLeft > 0) {
			  int n = ((IFileInputStream)input).readWithChecksum(buf, 0, (int) Math.min(bytesLeft, BYTES_TO_READ));
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
