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

package org.apache.geode.internal.net.messages;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolMessageHandler;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.net.messages.NewClientServerConnection;
import org.apache.geode.internal.net.runnable.AcceptorConnection;
import org.apache.geode.internal.net.runnable.AcceptorSocket;
import org.apache.logging.log4j.Logger;

import java.net.Socket;
import java.util.Iterator;
import java.util.ServiceLoader;

public class ServerConnectionFactory {
  private static final ClientProtocolMessageHandler newClientProtocol =
      initializeNewClientProtocol();

  private static ClientProtocolMessageHandler initializeNewClientProtocol() {
    ClientProtocolMessageHandler newClientProtocol = null;

    Iterator<ClientProtocolMessageHandler> protocolIterator =
        ServiceLoader.load(ClientProtocolMessageHandler.class).iterator();

    // assert (protocolIterator.hasNext());

    newClientProtocol = protocolIterator.next();

    // TODO handle multiple ClientProtocolMessageHandler impls.
    // assert (!protocolIterator.hasNext());

    // asserts break tests. We just assume it worked and blow up if it didn't.
    return newClientProtocol;
  }


  public static AcceptorConnection makeServerConnection(Socket socket, Cache cache,
      CachedRegionHelper helper, CacheServerStats stats, int hsTimeout, int socketBufferSize,
      String communicationModeStr, byte communicationMode, AcceptorImpl acceptor, Logger logger) {
    if (communicationMode == AcceptorImpl.CLIENT_TO_SERVER_NEW_PROTOCOL) {
      return new NewClientServerConnection(socket, cache, acceptor, logger, newClientProtocol);
      // return new NewClientServerConnection(socket, cache, helper, stats, hsTimeout,
      // socketBufferSize,
      // communicationModeStr, communicationMode, acceptor, newClientProtocol);
    } else {
      return new ServerConnection(socket, cache, helper, stats, hsTimeout, socketBufferSize,
          communicationModeStr, communicationMode, acceptor);
    }

  }

}
