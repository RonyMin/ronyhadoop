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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
  private final byte[] memory;
  private BoundedByteArrayOutputStream byteStream;
  // Decompression of map-outputs
  private final CompressionCodec codec;
  private final Decompressor decompressor;
  
  private Set<String> replicationTaskSet;
  private Configuration job;
  private String mapId;

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
    job.set("fs.defaultFS", "hdfs://10.150.20.22:8020");
    // job.set("fs.defaultFS", "hdfs://172.30.1.100:8020");
    updateReplicationMap();
  }

  public byte[] getMemory() {
    return memory;
  }

  public BoundedByteArrayOutputStream getArrayStream() {
    return byteStream;
  }

  @Override
  public void shuffle(MapHost host, InputStream input,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    IFileInputStream checksumIn = 
      new IFileInputStream(input, compressedLength, conf);

    input = checksumIn;       
  
    // Are map-outputs compressed?
    if (codec != null) {
      decompressor.reset();
      input = codec.createInputStream(input, decompressor);
    }
    
    LOG.info("Deciding replication status of incoming mapID: " + mapId);

    if(this.replicationTaskSet.contains(mapId)) {

    	FileSystem localFS = FileSystem.getLocal(job);
    	String targetFilePath = "/data/replication/"+mapId+"/file.out";
    	File targetFile = new File(targetFilePath);
    	DataInputStream dis = new DataInputStream(new FileInputStream(targetFile));

    	checksumIn = new IFileInputStream(dis, compressedLength, conf);
    	//checksumIn.disableChecksumValidation();
    	checksumIn.read(memory, 0, memory.length);

    	metrics.inputBytes(memory.length);
    	reporter.progress();
    	LOG.info("Read " + memory.length + " bytes from map-output for " +
                getMapId());
		
    } else {
    
    	try {
    		IOUtils.readFully(input, memory, 0, memory.length);
    		metrics.inputBytes(memory.length);
    		reporter.progress();
    		LOG.info("Read " + memory.length + " bytes from map-output for " +
                getMapId());

    		/**
    		 * We've gotten the amount of data we were expecting. Verify the
    		 * decompressor has nothing more to offer. This action also forces the
    		 * decompressor to read any trailing bytes that weren't critical
    		 * for decompression, which is necessary to keep the stream
    		 * in sync.
    		 */
    		if (input.read() >= 0 ) {
    			throw new IOException("Unexpected extra bytes from input stream for " +
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
  
  private FileSystem hdfs;
  private Path replicationStorePath;
  private FileStatus[] replicationTaskStatus;
  
  // Rony
  public void updateReplicationMap() {
	  
	  try {
		this.hdfs = FileSystem.get(job);
	  } catch (IOException e) {
		e.printStackTrace();
	  }
	  
	  this.replicationStorePath = new Path(job.get("fs.defaultFS")+"/replication");
	  LOG.info("ShuffleHandler: Building replication task set...");
	  try {
		this.replicationTaskStatus = hdfs.listStatus(replicationStorePath);
	  } catch (Exception e) {
		e.printStackTrace();
	  }
		
	  for(FileStatus fs : replicationTaskStatus) {
		String task = fs.getPath().toString().split("/")[4];
		if(!this.replicationTaskSet.contains(task)) {
		LOG.info("ShuffleHandler: task " + task + " is added into replication task set!!");
		this.replicationTaskSet.add(task);
	  }
	}  
  }
}
