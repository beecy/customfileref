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
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.stage.lib.sample.Errors;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.streamsets.stage.lib.sample.OutputFileRef;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Rot13Processor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(Rot13Processor.class);
  private static final String PERMISSIONS = "permissions";
  private static final String FILE = "file";
  private static final String FILE_NAME = "filename";

  /**
   * Gives access to the UI configuration of the stage provided by the {@link Rot13DProcessor} class.
   */
  public abstract String getDirectory();

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    // Check we can write to the configured directory
    File f = new File(getDirectory(), "dummy-" + UUID.randomUUID().toString());
    try {
      if (!f.getParentFile().exists() && !f.getParentFile().mkdirs()){
        issues.add(
            getContext().createConfigIssue(
                Groups.ROT13.name(), "directory", Errors.ROT13_00, f.getParentFile().getPath()
            )
        );
      } else if (!f.createNewFile()) {
        issues.add(
            getContext().createConfigIssue(
                Groups.ROT13.name(), "directory", Errors.ROT13_01, f.getPath()
            )
        );
      } else {
        f.delete();
      }
    } catch (SecurityException | IOException e) {
      LOG.error("Exception accessing directory", e);
      issues.add(
          getContext().createConfigIssue(
              Groups.ROT13.name(), "directory", Errors.ROT13_02, getDirectory(), e.getMessage(), e
          )
      );
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    // Get existing file's details
    String fileName = record.get("/fileInfo/filename").getValueAsString();
    FileRef fileRef = record.get("/fileRef").getValueAsFileRef();

    // Create a reference to an output file
    OutputFileRef outputFileRef;
    outputFileRef = new OutputFileRef(getDirectory(), fileName);

    // Read from incoming FileRef, write to output file
    File file = outputFileRef.getFile();
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file));
         InputStream is = fileRef.createInputStream(getContext(), InputStream.class)) {
      BufferedReader br = new BufferedReader(new InputStreamReader(is));

      // rot13 the data
      int ch;
      while ((ch = br.read()) != -1) {
        bw.write(rot13(ch));
      }

      // Close the output file now so that metadata is accurate
      bw.close();

      // Replace existing fileRef & fileInfo
      record.set("/fileRef", Field.create(outputFileRef));
      record.set("/fileInfo", createFieldForMetadata(getFileMetadata(file)));
    } catch (IOException e) {
      LOG.error("IOException", e);
      throw new OnRecordErrorException(record, Errors.ROT13_03, e.getMessage(), e);
    }

    // Emit the input record
    batchMaker.addRecord(record);
  }

  private static int rot13(int ch) {
    if ((ch >= 'a' && ch <= 'm') || (ch >= 'A' && ch <= 'M')) {
      return ch + 13;
    } else if ((ch >= 'n' && ch <= 'z') || (ch >= 'N' && ch <= 'Z')) {
      return ch - 13;
    }
    return ch;
  }

  // From com.streamsets.pipeline.stage.origin.spooldir.SpoolDirSource
  @NotNull
  private Map<String, Object> getFileMetadata(File file) throws IOException {
    boolean isPosix = file.toPath().getFileSystem().supportedFileAttributeViews().contains("posix");
    Map<String, Object> metadata = new HashMap<>(Files.readAttributes(file.toPath(), isPosix? "posix:*" : "*"));
    metadata.put(FILE_NAME, file.getName());
    metadata.put(FILE, file.getPath());
    if (isPosix && metadata.containsKey(PERMISSIONS) && Set.class.isAssignableFrom(metadata.get(PERMISSIONS).getClass())) {
      Set<PosixFilePermission> posixFilePermissions = (Set<PosixFilePermission>)(metadata.get(PERMISSIONS));
      //converts permission to rwx- format and replace it in permissions field.
      // (totally containing 9 characters 3 for user 3 for group and 3 for others)
      metadata.put(PERMISSIONS, PosixFilePermissions.toString(posixFilePermissions));
    }
    return metadata;
  }

  // From com.streamsets.pipeline.lib.io.fileref.FileRefUtil
  private static Field createFieldForMetadata(Object metadataObject) {
    if (metadataObject == null) {
      return Field.create("");
    }
    if (metadataObject instanceof Boolean) {
      return Field.create((Boolean) metadataObject);
    } else if (metadataObject instanceof Character) {
      return Field.create((Character) metadataObject);
    } else if (metadataObject instanceof Byte) {
      return Field.create((Byte) metadataObject);
    } else if (metadataObject instanceof Short) {
      return Field.create((Short) metadataObject);
    } else if (metadataObject instanceof Integer) {
      return Field.create((Integer) metadataObject);
    } else if (metadataObject instanceof Long) {
      return Field.create((Long) metadataObject);
    } else if (metadataObject instanceof Float) {
      return Field.create((Float) metadataObject);
    } else if (metadataObject instanceof Double) {
      return Field.create((Double) metadataObject);
    } else if (metadataObject instanceof Date) {
      return Field.createDatetime((Date) metadataObject);
    } else if (metadataObject instanceof BigDecimal) {
      return Field.create((BigDecimal) metadataObject);
    } else if (metadataObject instanceof String) {
      return Field.create((String) metadataObject);
    } else if (metadataObject instanceof byte[]) {
      return Field.create((byte[]) metadataObject);
    } else if (metadataObject instanceof Collection) {
      Iterator iterator = ((Collection)metadataObject).iterator();
      List<Field> fields = new ArrayList<>();
      while (iterator.hasNext()) {
        fields.add(createFieldForMetadata(iterator.next()));
      }
      return Field.create(fields);
    } else if (metadataObject instanceof Map) {
      boolean isListMap = (metadataObject instanceof LinkedHashMap);
      Map<String, Field> fieldMap = isListMap? new LinkedHashMap<String, Field>() : new HashMap<String, Field>();
      Map<Object, Object> map = (Map)metadataObject;
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        fieldMap.put(entry.getKey().toString(), createFieldForMetadata(entry.getValue()));
      }
      return isListMap? Field.create(Field.Type.LIST_MAP, fieldMap) : Field.create(fieldMap);
    } else {
      return Field.create(metadataObject.toString());
    }
  }
}