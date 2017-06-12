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

package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.security.SecurityService;

import java.net.Socket;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;

public class ServerConnectionFactory {
  private static ClientProtocolMessageHandler newClientProtocol = initializeNewClientProtocol();

  private static ClientProtocolMessageHandler initializeNewClientProtocol() {
    ClientProtocolMessageHandler newClientProtocol = null;

    Iterator<ClientProtocolMessageHandler> protocolIterator =
        ServiceLoader.load(ClientProtocolMessageHandler.class).iterator();

    // assert (protocolIterator.hasNext());

    try {
      newClientProtocol = protocolIterator.next();
    } catch (NoSuchElementException ignore) {
    }

    // TODO handle multiple ClientProtocolMessageHandler impls.
    // assert (!protocolIterator.hasNext()); breaks geode-core tests :(

    return newClientProtocol;
  }


  public static ServerConnection makeServerConnection(Socket s, InternalCache c,
      CachedRegionHelper helper, CacheServerStats stats, int hsTimeout, int socketBufferSize,
      String communicationModeStr, byte communicationMode, Acceptor acceptor,
      SecurityService securityService) {
    if (communicationMode == AcceptorImpl.CLIENT_TO_SERVER_NEW_PROTOCOL) {
      // assume if class wasn't present at startup, it won't be present now.
      if (newClientProtocol == null) {
        throw new RuntimeException("New client protocol not present in JVM.");
      }
      return new NewClientServerConnection(s, c, helper, stats, hsTimeout, socketBufferSize,
          communicationModeStr, communicationMode, acceptor, securityService, newClientProtocol);
    } else {
      return new LegacyServerConnection(s, c, helper, stats, hsTimeout, socketBufferSize,
          communicationModeStr, communicationMode, acceptor, securityService);
    }

  }

}
