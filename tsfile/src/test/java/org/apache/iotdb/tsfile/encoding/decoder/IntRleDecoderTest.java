/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.encoding.decoder;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.RleEncoder;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IntRleDecoderTest {

  private List<Integer> rleList;
  private List<Integer> bpList;
  private List<Integer> hybridList;
  private int rleBitWidth;
  private int bpBitWidth;
  private int hybridWidth;

  @Before
  public void setUp() throws Exception {
    rleList = new ArrayList<Integer>();
    int rleCount = 11;
    int rleNum = 18;
    int rleStart = 11;
    for (int i = 0; i < rleNum; i++) {
      for (int j = 0; j < rleCount; j++) {
        rleList.add(rleStart);
      }
      for (int j = 0; j < rleCount; j++) {
        rleList.add(rleStart - 1);
      }
      rleCount += 2;
      rleStart *= -3;
    }
    rleBitWidth = ReadWriteForEncodingUtils.getIntMaxBitWidth(rleList);

    bpList = new ArrayList<Integer>();
    int bpCount = 100000;
    int bpStart = 11;
    for (int i = 0; i < bpCount; i++) {
      bpStart += 3;
      if (i % 2 == 1) {
        bpList.add(bpStart * -1);
      } else {
        bpList.add(bpStart);
      }
    }
    bpBitWidth = ReadWriteForEncodingUtils.getIntMaxBitWidth(bpList);

    hybridList = new ArrayList<Integer>();
    int hybridCount = 11;
    int hybridNum = 1000;
    int hybridStart = 20;

    for (int i = 0; i < hybridNum; i++) {
      for (int j = 0; j < hybridCount; j++) {
        hybridStart += 3;
        if (j % 2 == 1) {
          hybridList.add(hybridStart * -1);
        } else {
          hybridList.add(hybridStart);
        }
      }
      for (int j = 0; j < hybridCount; j++) {
        if (i % 2 == 1) {
          hybridList.add(hybridStart * -1);
        } else {
          hybridList.add(hybridStart);
        }
      }
      hybridCount += 2;
    }
    hybridWidth = ReadWriteForEncodingUtils.getIntMaxBitWidth(hybridList);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testRleReadBigInt() throws IOException {
    List<Integer> list = new ArrayList<>();
    for (int i = 7000000; i < 10000000; i++) {
      list.add(i);
    }
    int width = ReadWriteForEncodingUtils.getIntMaxBitWidth(list);
    testLength(list, width, false, 1);
    for (int i = 1; i < 10; i++) {
      testLength(list, width, false, i);
    }
  }

  @Test
  public void testRleReadInt() throws IOException {
    for (int i = 1; i < 10; i++) {
      testLength(rleList, rleBitWidth, false, i);
    }
  }

  @Test
  public void testMaxRLERepeatNUM() throws IOException {
    List<Integer> repeatList = new ArrayList<>();
    int rleCount = 17;
    int rleNum = 5;
    int rleStart = 11;
    for (int i = 0; i < rleNum; i++) {
      for (int j = 0; j < rleCount; j++) {
        repeatList.add(rleStart);
      }
      for (int j = 0; j < rleCount; j++) {
        repeatList.add(rleStart / 3);
      }
      rleCount *= 7;
      rleStart *= -3;
    }
    int bitWidth = ReadWriteForEncodingUtils.getIntMaxBitWidth(repeatList);
    for (int i = 1; i < 10; i++) {
      testLength(repeatList, bitWidth, false, i);
    }
  }

  @Test
  public void testBitPackingReadInt() throws IOException {
    for (int i = 1; i < 10; i++) {
      testLength(bpList, bpBitWidth, false, i);
    }
  }

  @Test
  public void testHybridReadInt() throws IOException {
    for (int i = 1; i < 3; i++) {
      testLength(hybridList, hybridWidth, false, i);
    }
  }

  @Test
  public void testHybridReadBoolean() throws IOException {
    for (int i = 1; i < 10; i++) {
      testLength(hybridList, hybridWidth, false, i);
    }
  }

  @Test
  public void testBitPackingReadHeader() throws IOException {
    for (int i = 1; i < 505; i++) {
      testBitPackedReadHeader(i);
    }
  }

  public void testBooleanLength(List<Integer> list, int bitWidth, boolean isDebug, int repeatCount)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RleEncoder<Integer> encoder = new IntRleEncoder(EndianType.BIG_ENDIAN);
    for (int i = 0; i < repeatCount; i++) {
      for (int value : list) {
        if (value % 2 == 0) {
          encoder.encode(false, baos);
        } else {
          encoder.encode(true, baos);
        }

      }
      encoder.flush(baos);
    }

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    RleDecoder decoder = new IntRleDecoder(EndianType.BIG_ENDIAN);
    for (int i = 0; i < repeatCount; i++) {
      for (int value : list) {
        boolean value_ = decoder.readBoolean(buffer);
        if (isDebug) {
          System.out.println(value_ + "/" + value);
        }
        if (value % 2 == 0) {
          assertEquals(false, value_);
        } else {
          assertEquals(true, value_);
        }

      }
    }
  }

  public void testLength(List<Integer> list, int bitWidth, boolean isDebug, int repeatCount)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RleEncoder<Integer> encoder = new IntRleEncoder(EndianType.BIG_ENDIAN);
    for (int i = 0; i < repeatCount; i++) {
      for (int value : list) {
        encoder.encode(value, baos);
      }
      encoder.flush(baos);
    }

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    RleDecoder decoder = new IntRleDecoder(EndianType.BIG_ENDIAN);
    for (int i = 0; i < repeatCount; i++) {
      for (int value : list) {
        int value_ = decoder.readInt(buffer);
        if (isDebug) {
          System.out.println(value_ + "/" + value);
        }
        assertEquals(value, value_);
      }
    }
  }

  private void testBitPackedReadHeader(int num) throws IOException {
    List<Integer> list = new ArrayList<Integer>();

    for (int i = 0; i < num; i++) {
      list.add(i);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int bitWidth = ReadWriteForEncodingUtils.getIntMaxBitWidth(list);
    RleEncoder<Integer> encoder = new IntRleEncoder(EndianType.BIG_ENDIAN);
    for (int value : list) {
      encoder.encode(value, baos);
    }
    encoder.flush(baos);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ReadWriteForEncodingUtils.readUnsignedVarInt(bais);
    assertEquals(bitWidth, bais.read());
    int header = ReadWriteForEncodingUtils.readUnsignedVarInt(bais);
    int group = header >> 1;
    assertEquals(group, (num + 7) / 8);
    int lastBitPackedNum = bais.read();
    if (num % 8 == 0) {
      assertEquals(lastBitPackedNum, 8);
    } else {
      assertEquals(lastBitPackedNum, num % 8);
    }
  }
}
