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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
  private final OutputStream disk; 
  private long compressedSize;
  
  private Set<String> replicationTaskSet;
  private Configuration job;
  private String mapId;
  
  private Configuration hdfsConf = null;
  private FileSystem hdfs = null;
  private Path replicationStorePath;
  private FileStatus[] replicationTaskStatus = null;

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
    disk = fs.create(tmpOutputPath);
    
    this.hdfsConf = new Configuration();
    hdfsConf.set("fs.defaultFS", "hdfs://10.150.20.22:8020");
    this.mapId = getMapId().toString();
    this.job = new Configuration();
    this.replicationTaskSet = new HashSet<String>();
    updateReplicationMap();
    
  }

  @VisibleForTesting
  static Path getTempPath(Path outPath, int fetcher) {
    return outPath.suffix(String.valueOf(fetcher));
  }

  @Override
  public void shuffle(MapHost host, InputStream input,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
	  
	if(!replicationTaskSet.contains(mapId)) {
		
    // Copy data to local-disk
    long bytesLeft = compressedLength;
    try {
      final int BYTES_TO_READ = 64 * 1024;
      byte[] buf = new byte[BYTES_TO_READ];
      while (bytesLeft > 0) {
        int n = input.read(buf, 0, (int) Math.min(bytesLeft, BYTES_TO_READ));
        if (n < 0) {
          throw new IOException("read past end of stream reading " + 
                                getMapId());
        }
        disk.write(buf, 0, n);
        bytesLeft -= n;
        metrics.inputBytes(n);
        reporter.progress();
      }

      LOG.info("Read " + (compressedLength - bytesLeft) + 
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
	} else {
		  
	   Path targetMOFFilePath = new Path("/data/replication/"+getMapId()+"/file.out");
	   this.tmpOutputPath = targetMOFFilePath;
	   LOG.info("Read " + compressedLength + 
	            " bytes from map-output for " + getMapId() + " in local replication store");
       metrics.inputBytes(compressedLength);
       reporter.progress();
	}
    this.compressedSize = compressedLength;
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
  public void updateReplicationMap() {
	  
	  try {
		hdfs = FileSystem.get(hdfsConf);
	  } catch (IOException e) {
		e.printStackTrace();
	  }
	  
	  replicationStorePath = new Path(hdfsConf.get("fs.defaultFS")+"/replication");
	  LOG.info("ShuffleHandler: Building replication task set...");
	  try {
		replicationTaskStatus = hdfs.listStatus(replicationStorePath);
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
