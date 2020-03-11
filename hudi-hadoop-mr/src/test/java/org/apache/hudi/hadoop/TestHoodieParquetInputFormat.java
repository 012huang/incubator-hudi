/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.FSUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestHoodieParquetInputFormat {

  private HoodieParquetInputFormat inputFormat;
  private JobConf jobConf;

  @Before
  public void setUp() {
    inputFormat = new HoodieParquetInputFormat();
    jobConf = new JobConf();
    inputFormat.setConf(jobConf);
  }

  @Rule
  public TemporaryFolder basePath = new TemporaryFolder();

  @Test
  public void testInputFormatLoad() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, 10, "100");
    InputFormatTestUtil.commit(basePath, "100");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 10);
    assertEquals(10, inputSplits.length);

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);
  }

  @Test
  public void testInputFormatUpdates() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, 10, "100");
    InputFormatTestUtil.commit(basePath, "100");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);

    // update files
    InputFormatTestUtil.simulateUpdates(partitionDir, "100", 5, "200", true);
    // Before the commit
    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);
    ensureFilesInCommit("Commit 200 has not been committed. We should not see files from this commit", files, "200", 0);
    InputFormatTestUtil.commit(basePath, "200");
    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);
    ensureFilesInCommit("5 files have been updated to commit 200. We should see 5 files from commit 200 and 5 "
        + "files from 100 commit", files, "200", 5);
    ensureFilesInCommit("5 files have been updated to commit 200. We should see 5 files from commit 100 and 5 "
        + "files from 200 commit", files, "100", 5);
  }

  @Test
  public void testIncrementalSimple() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, 10, "100");
    createCommitFile(basePath, "100", "2016/05/01");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    InputFormatTestUtil.setupIncremental(jobConf, "100", 1);

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals("We should exclude commit 100 when returning incremental pull with start commit time as 100", 0,
        files.length);
  }

  private void createCommitFile(TemporaryFolder basePath, String commitNumber, String partitionPath)
      throws IOException {
    List<HoodieWriteStat> writeStats = HoodieTestUtils.generateFakeHoodieWriteStat(1);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    writeStats.forEach(stat -> commitMetadata.addWriteStat(partitionPath, stat));
    File file = new File(basePath.getRoot().toString() + "/.hoodie/", commitNumber + ".commit");
    file.createNewFile();
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    fileOutputStream.write(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8));
    fileOutputStream.flush();
    fileOutputStream.close();
  }

  private File createCompactionFile(TemporaryFolder basePath, String commitTime)
          throws IOException {
    File file = new File(basePath.getRoot().toString() + "/.hoodie/",
            HoodieTimeline.makeRequestedCompactionFileName(commitTime));
    assertTrue(file.createNewFile());
    FileOutputStream os = new FileOutputStream(file);
    try {
      HoodieCompactionPlan compactionPlan = HoodieCompactionPlan.newBuilder().setVersion(2).build();
      // Write empty commit metadata
      os.write(AvroUtils.serializeCompactionPlan(compactionPlan).get());
      return file;
    } finally {
      os.close();
    }
  }

  @Test
  public void testIncrementalWithMultipleCommits() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, 10, "100");
    createCommitFile(basePath, "100", "2016/05/01");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    // update files
    InputFormatTestUtil.simulateUpdates(partitionDir, "100", 5, "200", false);
    createCommitFile(basePath, "200", "2016/05/01");

    InputFormatTestUtil.simulateUpdates(partitionDir, "100", 4, "300", false);
    createCommitFile(basePath, "300", "2016/05/01");

    InputFormatTestUtil.simulateUpdates(partitionDir, "100", 3, "400", false);
    createCommitFile(basePath, "400", "2016/05/01");

    InputFormatTestUtil.simulateUpdates(partitionDir, "100", 2, "500", false);
    createCommitFile(basePath, "500", "2016/05/01");

    InputFormatTestUtil.simulateUpdates(partitionDir, "100", 1, "600", false);
    createCommitFile(basePath, "600", "2016/05/01");

    InputFormatTestUtil.setupIncremental(jobConf, "100", 1);
    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals("Pulling 1 commit from 100, should get us the 5 files committed at 200", 5, files.length);
    ensureFilesInCommit("Pulling 1 commit from 100, should get us the 5 files committed at 200", files, "200", 5);

    InputFormatTestUtil.setupIncremental(jobConf, "100", 3);
    files = inputFormat.listStatus(jobConf);

    assertEquals("Pulling 3 commits from 100, should get us the 3 files from 400 commit, 1 file from 300 "
        + "commit and 1 file from 200 commit", 5, files.length);
    ensureFilesInCommit("Pulling 3 commits from 100, should get us the 3 files from 400 commit", files, "400", 3);
    ensureFilesInCommit("Pulling 3 commits from 100, should get us the 1 files from 300 commit", files, "300", 1);
    ensureFilesInCommit("Pulling 3 commits from 100, should get us the 1 files from 200 commit", files, "200", 1);

    InputFormatTestUtil.setupIncremental(jobConf, "100", HoodieHiveUtil.MAX_COMMIT_ALL);
    files = inputFormat.listStatus(jobConf);

    assertEquals("Pulling all commits from 100, should get us the 1 file from each of 200,300,400,500,400 commits",
        5, files.length);
    ensureFilesInCommit("Pulling all commits from 100, should get us the 1 files from 600 commit", files, "600", 1);
    ensureFilesInCommit("Pulling all commits from 100, should get us the 1 files from 500 commit", files, "500", 1);
    ensureFilesInCommit("Pulling all commits from 100, should get us the 1 files from 400 commit", files, "400", 1);
    ensureFilesInCommit("Pulling all commits from 100, should get us the 1 files from 300 commit", files, "300", 1);
    ensureFilesInCommit("Pulling all commits from 100, should get us the 1 files from 200 commit", files, "200", 1);
  }

  // TODO enable this after enabling predicate pushdown
  public void testPredicatePushDown() throws IOException {
    // initial commit
    Schema schema = InputFormatTestUtil.readSchema("/sample1.avsc");
    String commit1 = "20160628071126";
    File partitionDir = InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, 10, commit1);
    InputFormatTestUtil.commit(basePath, commit1);
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    // check whether we have 10 records at this point
    ensureRecordsInCommit("We need to have 10 records at this point for commit " + commit1, commit1, 10, 10);

    // update 2 records in the original parquet file and save it as commit 200
    String commit2 = "20160629193623";
    InputFormatTestUtil.simulateParquetUpdates(partitionDir, schema, commit1, 10, 2, commit2);
    InputFormatTestUtil.commit(basePath, commit2);

    InputFormatTestUtil.setupIncremental(jobConf, commit1, 1);
    // check whether we have 2 records at this point
    ensureRecordsInCommit("We need to have 2 records that was modified at commit " + commit2 + " and no more", commit2,
        2, 2);
    // Make sure we have the 10 records if we roll back the stattime
    InputFormatTestUtil.setupIncremental(jobConf, "0", 2);
    ensureRecordsInCommit("We need to have 8 records that was modified at commit " + commit1 + " and no more", commit1,
        8, 10);
    ensureRecordsInCommit("We need to have 2 records that was modified at commit " + commit2 + " and no more", commit2,
        2, 10);
  }

  @Test
  public void testGetIncrementalTableNames() throws IOException {
    String[] expectedincrTables = {"db1.raw_trips", "db2.model_trips"};
    JobConf conf = new JobConf();
    String incrementalMode1 = String.format(HoodieHiveUtil.HOODIE_CONSUME_MODE_PATTERN, expectedincrTables[0]);
    conf.set(incrementalMode1, HoodieHiveUtil.INCREMENTAL_SCAN_MODE);
    String incrementalMode2 = String.format(HoodieHiveUtil.HOODIE_CONSUME_MODE_PATTERN, expectedincrTables[1]);
    conf.set(incrementalMode2,HoodieHiveUtil.INCREMENTAL_SCAN_MODE);
    String defaultmode = String.format(HoodieHiveUtil.HOODIE_CONSUME_MODE_PATTERN, "db3.first_trips");
    conf.set(defaultmode, HoodieHiveUtil.DEFAULT_SCAN_MODE);
    List<String> actualincrTables = HoodieHiveUtil.getIncrementalTableNames(Job.getInstance(conf));
    for (String expectedincrTable : expectedincrTables) {
      assertTrue(actualincrTables.contains(expectedincrTable));
    }
  }

  // test incremental read does not go past compaction instant for RO views
  @Test
  public void testIncrementalWithPendingCompaction() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, 10, "100");
    createCommitFile(basePath, "100", "2016/05/01");

    // simulate compaction requested at 300
    File compactionFile = createCompactionFile(basePath, "300");

    // write inserts into new bucket
    InputFormatTestUtil.simulateInserts(partitionDir, "fileId2", 10,  "400");
    createCommitFile(basePath, "400", "2016/05/01");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    InputFormatTestUtil.setupIncremental(jobConf, "0", -1);
    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals("Pulling all commit from beginning, should not return instants after begin compaction",
            10, files.length);
    ensureFilesInCommit("Pulling all commit from beginning, should not return instants after begin compaction",
            files, "100", 10);

    // delete compaction and verify inserts show up
    compactionFile.delete();
    InputFormatTestUtil.setupIncremental(jobConf, "0", -1);
    files = inputFormat.listStatus(jobConf);
    assertEquals("after deleting compaction, should get all inserted files",
            20, files.length);

    ensureFilesInCommit("Pulling all commit from beginning, should return instants before requested compaction",
            files, "100", 10);
    ensureFilesInCommit("Pulling all commit from beginning, should return instants after requested compaction",
            files, "400", 10);

  }

  private void ensureRecordsInCommit(String msg, String commit, int expectedNumberOfRecordsInCommit,
      int totalExpected) throws IOException {
    int actualCount = 0;
    int totalCount = 0;
    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
    for (InputSplit split : splits) {
      RecordReader<NullWritable, ArrayWritable> recordReader = inputFormat.getRecordReader(split, jobConf, null);
      NullWritable key = recordReader.createKey();
      ArrayWritable writable = recordReader.createValue();

      while (recordReader.next(key, writable)) {
        // writable returns an array with [field1, field2, _hoodie_commit_time,
        // _hoodie_commit_seqno]
        // Take the commit time and compare with the one we are interested in
        if (commit.equals((writable.get()[2]).toString())) {
          actualCount++;
        }
        totalCount++;
      }
    }
    assertEquals(msg, expectedNumberOfRecordsInCommit, actualCount);
    assertEquals(msg, totalExpected, totalCount);
  }

  public static void ensureFilesInCommit(String msg, FileStatus[] files, String commit, int expected) {
    int count = 0;
    for (FileStatus file : files) {
      String commitTs = FSUtils.getCommitTime(file.getPath().getName());
      if (commit.equals(commitTs)) {
        count++;
      }
    }
    assertEquals(msg, expected, count);
  }
}
