/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.protocol.protobuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.google.protobuf.GeneratedMessageV3;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.tier.sockets.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.security.NoOpAuthorizer;
import org.apache.geode.internal.protocol.protobuf.statistics.NoOpProtobufStatistics;

public class ProtobufTestUtilities {
  public static ByteArrayInputStream messageToByteArrayInputStream(GeneratedMessageV3 message)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    message.writeDelimitedTo(byteArrayOutputStream);
    return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
  }

  public static ClientProtocol.Request createProtobufRequestWithGetRegionNamesRequest(
      RegionAPI.GetRegionNamesRequest getRegionNamesRequest) {
    return ClientProtocol.Request.newBuilder().setGetRegionNamesRequest(getRegionNamesRequest)
        .build();
  }

  public static MessageExecutionContext getNoAuthExecutionContext(Cache cache) {
    return new MessageExecutionContext(cache, new NoOpAuthorizer(), new Object(),
        new NoOpProtobufStatistics());
  }
}
