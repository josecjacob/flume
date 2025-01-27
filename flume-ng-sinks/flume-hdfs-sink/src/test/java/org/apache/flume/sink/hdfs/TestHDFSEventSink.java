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
package org.apache.flume.sink.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHDFSEventSink {

  private HDFSEventSink sink;
  private String testPath;
  private static final Logger LOG = LoggerFactory
      .getLogger(HDFSEventSink.class);

  private void dirCleanup() {
    Configuration conf = new Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);
      Path dirPath = new Path(testPath);
      fs.delete(dirPath, true);
    } catch (IOException eIO) {
      LOG.warn("IO Error in test cleanup", eIO);
    }
  }

  // TODO: use System.getProperty("file.separator") instead of hardcoded '/'
  @Before
  public void setUp() {
    /*
     * FIXME: Use a dynamic path to support concurrent test execution. Also,
     * beware of the case where this path is used for something or when the
     * Hadoop config points at file:/// rather than hdfs://. We need to find a
     * better way of testing HDFS related functionality.
     */
    testPath = "file:///tmp/fluem-test."
        + Calendar.getInstance().getTimeInMillis() + "."
        + Thread.currentThread().getId();

    sink = new HDFSEventSink();
    dirCleanup();
  }

  @After
  public void tearDown() {
    if (System.getenv("hdfs_keepFiles") == null)
      dirCleanup();
  }

  @Test
  public void testLifecycle() throws InterruptedException, LifecycleException {
    Context context = new Context();

    context.put("hdfs.path", testPath);
    /*
     * context.put("hdfs.rollInterval", String.class);
     * context.get("hdfs.rollSize", String.class); context.get("hdfs.rollCount",
     * String.class);
     */
    Configurables.configure(sink, context);

    sink.setChannel(new MemoryChannel());

    sink.start();
    sink.stop();
  }

  @Test
  public void testTextAppend() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    final long txnMax = 25;
    final long rollCount = 3;
    final long batchSize = 2;
    final String fileName = "FlumeData";
    String newPath = testPath + "/singleTextBucket";
    int totalEvents = 0;
    int i = 1, j = 1;

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    // context.put("hdfs.path", testPath + "/%Y-%m-%d/%H");
    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.txnEventMax", String.valueOf(txnMax));
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.writeFormat", "Text");
    context.put("hdfs.fileType", "DataStream");

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();

    // push the event batches into channel
    for (i = 1; i < 4; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (j = 1; j <= txnMax; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);

        event.setBody(("Test." + i + "." + j).getBytes());
        channel.put(event);
        totalEvents++;
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();

    // loop through all the files generated and check their contains
    FileStatus[] dirStat = fs.listStatus(dirPath);
    Path fList[] = FileUtil.stat2Paths(dirStat);

    // check that the roll happened correctly for the given data
    // Note that we'll end up with one last file with only header
    Assert.assertEquals((totalEvents / rollCount) + 1, fList.length);

    try {
      i = j = 1;
      for (int cnt = 0; cnt < fList.length - 1; cnt++) {
        Path filePath = new Path(newPath + "/" + fileName + "." + cnt);
        FSDataInputStream input = fs.open(filePath);
        BufferedReader d = new BufferedReader(new InputStreamReader(input));
        String line;

        while ((line = d.readLine()) != null) {
          Assert.assertEquals(line, ("Test." + i + "." + j));
          if (++j > txnMax) {
            j = 1;
            i++;
          }
        }
        input.close();
      }
    } catch (IOException ioe) {
      System.err.println("IOException during operation: " + ioe.toString());
      return;
    }
    Assert.assertEquals(i, 4);
  }

  @Test
  public void testSimpleAppend() throws InterruptedException,
      LifecycleException, EventDeliveryException, IOException {

    final long txnMax = 25;
    final String fileName = "FlumeData";
    final long rollCount = 5;
    final long batchSize = 2;
    final int numBatches = 4;
    String newPath = testPath + "/singleBucket";
    int totalEvents = 0;
    int i = 1, j = 1;

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.txnEventMax", String.valueOf(txnMax));
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);

    Calendar eventDate = Calendar.getInstance();

    // push the event batches into channel
    for (i = 1; i < numBatches; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (j = 1; j <= txnMax; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);

        event.setBody(("Test." + i + "." + j).getBytes());
        channel.put(event);
        totalEvents++;
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();

    // loop through all the files generated and check their contains
    FileStatus[] dirStat = fs.listStatus(dirPath);
    Path fList[] = FileUtil.stat2Paths(dirStat);

    // check that the roll happened correctly for the given data
    // Note that we'll end up with one last file with only header
    Assert.assertEquals((totalEvents / rollCount) + 1, fList.length);

    try {
      i = j = 1;
      for (int cnt = 0; cnt < fList.length - 1; cnt++) {
        Path filePath = new Path(newPath + "/" + fileName + "." + cnt);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, filePath, conf);
        LongWritable key = new LongWritable();
        BytesWritable value = new BytesWritable();
        BytesWritable expValue;

        while (reader.next(key, value)) {
          expValue = new BytesWritable(("Test." + i + "." + j).getBytes());
          Assert.assertEquals(expValue, value);
          if (++j > txnMax) {
            j = 1;
            i++;
          }
        }
        reader.close();
      }
    } catch (IOException ioe) {
      System.err.println("IOException during operation: " + ioe.toString());
      System.exit(1);
    }
    Assert.assertEquals(i, 4);

  }

  @Test
  public void testAppend() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    final long txnMax = 25;
    final long rollCount = 3;
    final long batchSize = 2;
    final String fileName = "FlumeData";

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(testPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    context.put("hdfs.path", testPath + "/%Y-%m-%d/%H");
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.txnEventMax", String.valueOf(txnMax));
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();

    // push the event batches into channel
    for (int i = 1; i < 4; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (int j = 1; j <= txnMax; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);

        event.setBody(("Test." + i + "." + j).getBytes());
        channel.put(event);
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();

    /*
     * 
     * // loop through all the files generated and check their contains
     * FileStatus[] dirStat = fs.listStatus(dirPath); Path fList[] =
     * FileUtil.stat2Paths(dirStat);
     * 
     * try { for (int cnt = 0; cnt < fList.length; cnt++) { SequenceFile.Reader
     * reader = new SequenceFile.Reader(fs, fList[cnt], conf); LongWritable key
     * = new LongWritable(); BytesWritable value = new BytesWritable();
     * 
     * while (reader.next(key, value)) { logger.info(key+ ":" +
     * value.toString()); } reader.close(); } } catch (IOException ioe) {
     * System.err.println("IOException during operation: " + ioe.toString());
     * System.exit(1); }
     */
  }

  // inject fault and make sure that the txn is rolled back and retried
  @Test
  public void testBadSimpleAppend() throws InterruptedException,
      LifecycleException, EventDeliveryException, IOException {

    final long txnMax = 25;
    final String fileName = "FlumeData";
    final long rollCount = 5;
    final long batchSize = 2;
    final int numBatches = 4;
    String newPath = testPath + "/singleBucket";
    int totalEvents = 0;
    int i = 1, j = 1;

    HDFSBadWriterFactory badWriterFactory = new HDFSBadWriterFactory();
    sink = new HDFSEventSink(badWriterFactory);

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.txnEventMax", String.valueOf(txnMax));
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.fileType", HDFSBadWriterFactory.BadSequenceFileType);

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);

    Calendar eventDate = Calendar.getInstance();

    // push the event batches into channel
    for (i = 1; i < numBatches; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (j = 1; j <= txnMax; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);

        event.setBody(("Test." + i + "." + j).getBytes());
        // inject fault
        if ((totalEvents % 30) == 1) {
          event.getHeaders().put("fault-once", "");
        }
        channel.put(event);
        totalEvents++;
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }
    LOG.info("clear any events pending due to errors");
    // clear any events pending due to errors
    sink.process();

    sink.stop();
  }

}
