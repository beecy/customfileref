/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.stage.processor.sample;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.stage.lib.sample.OutputFileRef;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;

public class TestRot13Processor {

  private static final String FILENAME = "testfile";
  private static final String DATA = "The quick brown fox jumps over the lazy dog.";
  private static final String ROT13_DATA = "Gur dhvpx oebja sbk whzcf bire gur ynml qbt.";

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessor() throws Exception {
    // Make test directories
    Path in = Files.createTempDirectory("in");
    Path out = Files.createTempDirectory("out");

    ProcessorRunner runner = new ProcessorRunner.Builder(Rot13DProcessor.class)
        .addConfiguration("directory", out.toString())
        .addOutputLane("output")
        .build();

    runner.runInit();

    try {
      Record record = RecordCreator.create();

      // Use OutputFileRef to create an incoming file
      OutputFileRef ofr = new OutputFileRef(in.toString(), FILENAME);

      // Write test data
      File f = ofr.getFile();
      Files.write(f.toPath(), DATA.getBytes());

      // Set up a whole file record
      HashMap<String, Field> root = new HashMap<>();
      root.put("fileRef", Field.create(ofr));
      record.set("/", Field.create(root));

      HashMap<String, Field> fileInfo = new HashMap<>();
      fileInfo.put("size", Field.create(f.length()));
      fileInfo.put("filename", Field.create(f.getName()));
      fileInfo.put("file", Field.create(f.getPath()));
      record.set("/fileInfo", Field.create(fileInfo));

      // Run the pipeline
      StageRunner.Output output = runner.runProcess(Arrays.asList(record));

      // We should have exactly one output record, with the expected filename
      Assert.assertEquals(1, output.getRecords().get("output").size());
      Assert.assertEquals(FILENAME, output.getRecords().get("output").get(0).get("/fileInfo/filename").getValue());

      // Get the output FileRef
      FileRef fr = output.getRecords().get("output").get(0).get("/fileRef").getValueAsFileRef();

      // Read the data
      InputStream is = fr.createInputStream(runner.getContext(), InputStream.class);
      byte[] bytes = new byte[ROT13_DATA.length()];
      is.read(bytes);
      int next = is.read();
      is.close();

      // Output should be rot13 of the input
      Assert.assertEquals(ROT13_DATA, new String(bytes));

      // Shouldn't be any more data
      Assert.assertEquals(-1, next);
    } finally {
      runner.runDestroy();
    }
  }
}
