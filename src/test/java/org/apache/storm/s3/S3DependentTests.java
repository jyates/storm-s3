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
package org.apache.storm.s3;

/**
 * Marker interface for tests that are dependent on S3 so they can be run separately
 * <p>
 * Tests that have this marker require an ~/.aws/credentials file:
 * <pre>
 * [aws-testing]
 * aws_access_key_id=[ACCESS_KEY]
 * aws_secret_access_key=[SECRET_KEY]
 * </pre>
 * That same user/role also needs to be able to have S3 permissions:
 * <ol>
 * <li>CreateBucket</li>
 * <li>PutObject</li>
 * <li>DeleteBucket</li>
 * <li>DeleteObject</li>
 * </ol>
 * </p>
 */
public interface S3DependentTests {
}
