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
package org.apache.geode.internal.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolPipeline;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolService;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolStatistics;
import org.apache.geode.internal.cache.tier.sockets.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.security.Authorizer;
import org.apache.geode.internal.protocol.protobuf.security.InvalidConfigAuthenticator;
import org.apache.geode.internal.protocol.protobuf.security.NoOpAuthorizer;
import org.apache.geode.internal.protocol.protobuf.security.ProtobufSimpleAuthenticator;
import org.apache.geode.internal.protocol.protobuf.ProtobufStreamProcessor;
import org.apache.geode.internal.protocol.protobuf.security.ProtobufSimpleAuthorizer;
import org.apache.geode.internal.protocol.protobuf.statistics.NoOpStatistics;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatistics;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatisticsImpl;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.protocol.protobuf.security.Authenticator;
import org.apache.geode.internal.protocol.protobuf.security.NoOpAuthenticator;

public class ProtobufProtocolService implements ClientProtocolService {
  private ProtobufClientStatistics statistics;
  private final ProtobufStreamProcessor protobufStreamProcessor = new ProtobufStreamProcessor();

  @Override
  public void initializeStatistics(String statisticsName, StatisticsFactory factory) {
    statistics = new ProtobufClientStatisticsImpl(factory, statisticsName, "ProtobufServerStats");
  }

  @Override
  public ClientProtocolStatistics getStatistics() {
    return statistics;
  }

  @Override
  public ClientProtocolPipeline createCachePipeline(Cache cache, SecurityService securityService) {
    assert (statistics != null);

    Authenticator authenticator = getAuthenticator(securityService);
    Authorizer authorizer = getAuthorizer(securityService);

    return new ProtobufPipeline(protobufStreamProcessor, statistics, cache, authenticator, authorizer,
        securityService);
  }

  @Override
  public void serveLocatorMessage(InputStream inputStream, OutputStream outputStream,
      InternalLocator locator) throws IOException {
    ProtobufClientStatistics statistics =
        this.statistics == null ? new NoOpStatistics() : this.statistics;

    statistics.clientConnected();

    protobufStreamProcessor.receiveMessage(inputStream, outputStream,
        new MessageExecutionContext(locator, statistics));

    statistics.clientDisconnected();
  }

  private Authenticator getAuthenticator(SecurityService securityService) {
    if (securityService.isIntegratedSecurity()) {
      // Simple authenticator...normal shiro
      return new ProtobufSimpleAuthenticator();
    }
    if (securityService.isPeerSecurityRequired() || securityService.isClientSecurityRequired()) {
      // Failing authentication...legacy security
      return new InvalidConfigAuthenticator();
    } else {
      // Noop authenticator...no security
      return new NoOpAuthenticator();
    }
  }

  private Authorizer getAuthorizer(SecurityService securityService) {
    if (securityService.isIntegratedSecurity()) {
      // Simple authenticator...normal shiro
      return new ProtobufSimpleAuthorizer(securityService);
    }
    if (securityService.isPeerSecurityRequired() || securityService.isClientSecurityRequired()) {
      // Failing authentication...legacy security
      // This should never be called.
      return null;
    } else {
      // Noop authenticator...no security
      return new NoOpAuthorizer();
    }
  }
}
