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
package org.apache.storm.s3.format;

import org.apache.storm.guava.base.Preconditions;
import org.apache.storm.s3.output.ContentEncoding;

import java.io.Serializable;
import java.util.Map;

/**
 *
 */
public class S3OutputConfiguration implements Serializable {
  private String bucket;
  private String contentType = "text/plain";
  private String path;
  private ContentEncoding encoding = ContentEncoding.NONE;

  public S3OutputConfiguration setBucket(String bucket) {
    this.bucket = bucket;
    return this;
  }

  public S3OutputConfiguration setContentType(String contentType) {
    this.contentType = contentType;
    return this;
  }

  public S3OutputConfiguration withPath(String path) {
    this.path = path;
    return this;
  }

  public S3OutputConfiguration withContentEncoding(ContentEncoding encoding){
    this.encoding = encoding;
    return this;
  }

  public void prepare(Map stormConf) {
    Preconditions.checkNotNull(bucket, "Bucket name must be specified.");
  }

  public String getBucket() {
    return bucket;
  }

  public String getContentType() {
    return contentType;
  }

  public ContentEncoding getEncoding() {
    return this.encoding;
  }
}
