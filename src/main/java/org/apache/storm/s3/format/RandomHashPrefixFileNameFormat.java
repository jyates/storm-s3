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

import org.apache.storm.guava.annotations.VisibleForTesting;
import org.apache.storm.guava.base.Preconditions;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;

/**
 * Prefixes the file name in the path with a random ASCII hash. This has is placed <i>before the
 * prefix specified in {@link #withPrefix(String)}</i>.
 * <p>
 * This class supports the
 * <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html">
 * recommendations from Amazon</a> to prefix keys with a hash to
 * get better s3 write performance, particularly if "your workload in an Amazon S3 bucket
 * routinely exceeds 100 PUT/LIST/DELETE requests per second or more than 300 GET requests per
 * second"
 * </p>
 */
public class RandomHashPrefixFileNameFormat extends DefaultFileNameFormat {

    private int prefixLength = 4;

    @Override
    public String getName(Object key, String identifier, long rotation, long timeStamp) {
        String basePrefix = this.prefix;
        try {
            String name = super.getName(key, identifier, rotation, timeStamp);
            String hex = toHex(name);
            this.prefix = basePrefix + hex.substring(0, prefixLength);
            return super.getName(key, identifier, rotation, timeStamp);
        } finally {
            this.prefix = basePrefix;
        }
    }


    public String toHex(String arg) {
        return String.format("%x", new BigInteger(1, arg.getBytes()));
    }

    public RandomHashPrefixFileNameFormat withPrefixLength(int length) {
        Preconditions.checkArgument(length >= 0);
        this.prefixLength = length;
        return this;
    }

    @VisibleForTesting
    public String getPathForTesting() {
        return this.path;
    }
}
