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

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.security.SecurityService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.ServiceLoader;

public class NewClientServerConnection extends ServerConnection {
  // The new protocol lives in a separate module and gets loaded when this class is instantiated.
  private final ClientProtocolMessageHandler newClientProtocol;


  /**
   * Creates a new <code>NewClientServerConnection</code> that processes messages received from an
   * edge client over a given <code>Socket</code>.
   *
   * @param s
   * @param c
   * @param helper
   * @param stats
   * @param hsTimeout
   * @param socketBufferSize
   * @param communicationModeStr
   * @param communicationMode
   * @param acceptor
   */
  public NewClientServerConnection(Socket s, InternalCache c, CachedRegionHelper helper,
      CacheServerStats stats, int hsTimeout, int socketBufferSize, String communicationModeStr,
      byte communicationMode, Acceptor acceptor, SecurityService securityService,
      ClientProtocolMessageHandler newClientProtocol) {
    super(s, c, helper, stats, hsTimeout, socketBufferSize, communicationModeStr, communicationMode,
        acceptor, securityService);
    assert (communicationMode == AcceptorImpl.CLIENT_TO_SERVER_NEW_PROTOCOL);
    this.newClientProtocol = newClientProtocol;
  }

  @Override
  protected void doOneMessage() {
    try {
      Socket socket = this.getSocket();
      InputStream inputStream = socket.getInputStream();
      OutputStream outputStream = socket.getOutputStream();
      // TODO serialization types?
      newClientProtocol.receiveMessage(inputStream, outputStream, this.getCache());
    } catch (IOException e) {
      // TODO?
    }
    return;
  }

  @Override
  protected boolean doAcceptHandShake(byte epType, int qSize) {
    // no handshake for new client protocol.
    return true;
  }

  @Override
  public boolean isClientServerConnection() {
    return true;
  }

  protected boolean createClientHandshake() {
    InetSocketAddress remoteAddress = (InetSocketAddress) getSocket().getRemoteSocketAddress();
    DistributedMember member =
        new InternalDistributedMember(remoteAddress.getAddress(), remoteAddress.getPort());
    this.proxyId = new ClientProxyMembershipID(member);
    this.handshake = new HandShake(this.proxyId, this.getDistributedSystem(), Version.CURRENT);
    return true;
  }


}
