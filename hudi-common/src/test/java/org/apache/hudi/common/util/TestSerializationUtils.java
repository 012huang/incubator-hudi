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

package org.apache.hudi.common.util;

import org.apache.avro.util.Utf8;
import org.apache.hudi.avro.model.HoodieReplaceMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.junit.jupiter.api.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests serialization utils.
 */
public class TestSerializationUtils {

  @Test
  public void testSerDeser() throws IOException {
    // It should handle null object references.
    verifyObject(null);
    // Object with nulls.
    verifyObject(new NonSerializableClass(null));
    // Object with valid values & no default constructor.
    verifyObject(new NonSerializableClass("testValue"));
    // Object with multiple constructor
    verifyObject(new NonSerializableClass("testValue1", "testValue2"));
    // Object which is of non-serializable class.
    verifyObject(new Utf8("test-key"));
    // Verify serialization of list.
    verifyObject(new LinkedList<>(Arrays.asList(2, 3, 5)));
  }

  @Test
  public void testTimeTakenReplaceMetadata() throws Exception {
    Random  r = new Random();
    int numPartitions = 10;
    HoodieReplaceMetadata replaceMetadata = new HoodieReplaceMetadata();
    int numFiles = 300000;
    Map<String, List<String>> partitionToReplaceFileId = new HashMap<>();

    for (int i = 0; i < numFiles; i++) {
      int partition = r.nextInt(numPartitions);
      String partitionStr = "2020/07/0" + partition;
      partitionToReplaceFileId.putIfAbsent(partitionStr, new ArrayList<>());
      partitionToReplaceFileId.get(partitionStr).add(FSUtils.createNewFileIdPfx());
    }
    replaceMetadata.setVersion(1);
    replaceMetadata.setTotalFilesReplaced(numFiles);
    replaceMetadata.setPolicy("insert_overwrite");
    replaceMetadata.setPartitionMetadata(partitionToReplaceFileId);

    long durationSerialization = 0;
    String filePath = "/tmp/myreplacetest";
    for (int i = 0; i < 10; i++) {
      long start = System.currentTimeMillis();
      byte[] data = TimelineMetadataUtils.serializeReplaceMetadata(replaceMetadata).get();
      String filePathI = "/tmp/myreplacetest" + i;
      FileOutputStream fos = new FileOutputStream(filePathI);
      fos.write(data);
      fos.close();
      durationSerialization += (System.currentTimeMillis() - start);
    }
    System.out.println("Took " + durationSerialization / 10 + " for serialization");


    long durationDeserialization = 0;
    long totalSize = 0;
    for (int i = 0; i < 10; i++) {
      String filePathI = "/tmp/myreplacetest" + i;
      long startD = System.currentTimeMillis();
      byte[] data = Files.readAllBytes(Paths.get(filePathI));
      HoodieReplaceMetadata replaceMetadataD = TimelineMetadataUtils.deserializeHoodieReplaceMetadata(data);
      assertEquals(replaceMetadataD.getTotalFilesReplaced(), numFiles);
      durationDeserialization += (System.currentTimeMillis() - startD);
      totalSize = ObjectSizeCalculator.getObjectSize(replaceMetadataD) + 2 * data.length;
    }
    System.out.println("Took " + durationDeserialization / 10 + " for deserialization, size " + totalSize + " bytes");
  }

  private <T> void verifyObject(T expectedValue) throws IOException {
    byte[] serializedObject = SerializationUtils.serialize(expectedValue);
    assertNotNull(serializedObject);
    assertTrue(serializedObject.length > 0);

    final T deserializedValue = SerializationUtils.<T>deserialize(serializedObject);
    if (expectedValue == null) {
      assertNull(deserializedValue);
    } else {
      assertEquals(expectedValue, deserializedValue);
    }
  }

  private static class NonSerializableClass {
    private String id;
    private String name;

    NonSerializableClass(String id) {
      this(id, "");
    }

    NonSerializableClass(String id, String name) {
      this.id = id;
      this.name = name;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof NonSerializableClass)) {
        return false;
      }
      final NonSerializableClass other = (NonSerializableClass) obj;
      return Objects.equals(this.id, other.id) && Objects.equals(this.name, other.name);
    }
  }
}
