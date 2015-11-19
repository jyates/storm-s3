/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.s3.output;

import org.apache.storm.s3.format.AbstractFileNameFormat;
import org.apache.storm.s3.format.S3OutputConfiguration;
import org.apache.storm.s3.output.upload.Uploader;

import java.io.IOException;
import java.util.Map;

/**
 * Builder for a given output stream. Supports things like compressing the input stream before
 * sending it to S3
 */
public class OutputStreamBuilder {

  public static final String ENCODING_KEY = "CONTENT_ENCODING";
  private final Uploader uploader;
  private final AbstractFileNameFormat format;
  private final S3OutputConfiguration s3;
  private String identifier = "";

  public OutputStreamBuilder(Uploader uploader, S3OutputConfiguration s3Info, String identifier,
      AbstractFileNameFormat
          fileNameFormat) {
    this.uploader = uploader;
    this.format = fileNameFormat;
    this.s3 = s3Info;
    this.identifier = identifier;
  }

  public S3MemBufferedOutputStream build(int rotationNumber) throws IOException {
    ContentEncoding encoding = s3.getEncoding();
    return
        new S3MemBufferedOutputStream<>(uploader, s3.getBucket(), format, s3.getContentType(),
              encoding, identifier, rotationNumber);
  }


  public static void setEncoding(Map conf, ContentEncoding encoding) {
    conf.put(ENCODING_KEY, encoding.name());
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }
}
