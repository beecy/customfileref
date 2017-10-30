package com.streamsets.stage.lib.sample;

import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Stage;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

// Simple FileRef implementation to allow a custom processor to
// write to a whole file
public class OutputFileRef extends FileRef {
  private static final Set<Class<? extends AutoCloseable>> supportedStreamClasses = Collections.singleton(InputStream.class);
  private static final int BUFFER_SIZE = 1024;
  File file;

  public OutputFileRef(String directory, String filename) {
    super(BUFFER_SIZE);
    file = new File(directory, filename);
  }

  @Override
  public <T extends AutoCloseable> Set<Class<T>> getSupportedStreamClasses() {
    return (Set)supportedStreamClasses;
  }

  @Override
  public <T extends AutoCloseable> T createInputStream(
      Stage.Context context, Class<T> aClass
  ) throws IOException {
    return (T)new FileInputStream(file);
  }

  public File getFile() {
    return file;
  }
}
