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


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RandomHashPrefixFileNameFormatTest {
    private String id = "id";
    private long rotation = 0, ts = 1;

    @Test
    public void testReproducibleHash(){
        RandomHashPrefixFileNameFormat format = new RandomHashPrefixFileNameFormat();
        assertEquals("Same file name not produced by two calls to the formatter",
              getName(format), getName(format));
    }

    private String getName(RandomHashPrefixFileNameFormat format) {
        return format.getName(null, id, rotation, ts);
    }

    @Test
    public void testPrefixLength(){
        RandomHashPrefixFileNameFormat format = new RandomHashPrefixFileNameFormat();
        format.withPrefixLength(0);
        String base = removePath(getName(format), format);

        RandomHashPrefixFileNameFormat format2 = new RandomHashPrefixFileNameFormat();
        int prefixLength = 4;
        format.withPrefixLength(prefixLength);
        String prefixed = removePath(getName(format2), format2);

        assertEquals(base, prefixed.substring(prefixLength));
    }

    private String removePath(String name, RandomHashPrefixFileNameFormat format){
        return name.replaceFirst(format.getPathForTesting(), "");
    }
}
