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

import org.apache.geode.CancelException;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolMessageHandler;
import org.apache.geode.internal.net.runnable.AcceptorSocket;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class NewClientServerConnection extends AcceptorSocket {
  // The new protocol lives in a separate module and gets loaded when this class is instantiated.
  private final ClientProtocolMessageHandler newClientProtocol;
  private final Cache cache;
  private final CachedRegionHelper cachedRegionHelper;

  public NewClientServerConnection(Socket socket, Cache cache, AcceptorImpl acceptor, Logger logger,
                                   CachedRegionHelper crhelper, ClientProtocolMessageHandler newClientProtocol) {
    super(acceptor, logger, socket);
    this.cache = cache;
    this.newClientProtocol = newClientProtocol;
    this.cachedRegionHelper = crhelper;
  }

  // /**
  // * Creates a new <code>NewClientServerConnection</code> that processes messages received from an
  // * edge client over a given <code>Socket</code>.
  // *
  // * @param s
  // * @param c
  // * @param helper
  // * @param stats
  // * @param hsTimeout
  // * @param socketBufferSize
  // * @param communicationModeStr
  // * @param communicationMode
  // * @param acceptor
  // */
  // public NewClientServerConnection(Socket s, Cache c, CachedRegionHelper helper,
  // CacheServerStats stats, int hsTimeout, int socketBufferSize, String communicationModeStr,
  // byte communicationMode, Acceptor acceptor, ClientProtocolMessageHandler newClientProtocol) {
  // super(s, c, helper, stats, hsTimeout, socketBufferSize, communicationModeStr,
  // communicationMode,
  // acceptor);
  // assert (communicationMode == AcceptorImpl.CLIENT_TO_SERVER_NEW_PROTOCOL);
  // this.newClientProtocol = newClientProtocol;
  // }

  @Override
  protected void doOneMessage() {
    try {
      InputStream inputStream = socket.getInputStream();
      OutputStream outputStream = socket.getOutputStream();
      // TODO serialization types?
      newClientProtocol.receiveMessage(inputStream, outputStream, this.cache);
    } catch (IOException e) {
      throw new RuntimeException(e);
      // TODO?
    }
    return;
  }

  @Override
  public void handleTermination() {
    // stats here later or something?
  }

  @Override
  public boolean cleanup() {
    try {
      socket.close();
    } catch (IOException ignore) {

    }
    return true;
  }

  @Override
  public void run() {
    if (acceptor.isSelector()) {
      runSingleMessage();
    } else {
      runAsThread();
    }
  }

  private void runSingleMessage() {
    try {
      // this.stats.decThreadQueueSize();
      // if (!isTerminated()) {
      // Message.setTLCommBuffer(this.acceptor.takeCommBuffer());
      if (isRunning()) {
        doOneMessage();
        registerWithSelector(); // finished msg so reregister
      }
    } catch (CancelException e) {
      // TODO : do we really need CancelException?
      // ok shutting down
      // ok shutting down
      // } catch (IOException ex) {
      // logger.warn(
      // LocalizedMessage.create(LocalizedStrings.ServerConnection_0__UNEXPECTED_EXCEPTION, ex));
      // setClientDisconnectedException(ex);
      // } finally {
      // this.acceptor.releaseCommBuffer(Message.setTLCommBuffer(null));
      // DistributedSystem.releaseThreadsSockets();
      // unsetOwner();
      // setNotProcessingMessage();
      // unset request specific timeout
      // this.unsetRequestSpecificTimeout(); todo?
      // if (!finishedMsg) {
      // try {
      // handleTermination();
      // } catch (CancelException e) {
      // ignore
      // }
      // }
      // }
      logger.error(e.toString());
    } catch (IOException e) {
      logger.error(e.toString());
    }
  }

  private void runAsThread() {
    try {
      while (isRunning()) {
        cachedRegionHelper.checkCancelInProgress(null);
        doOneMessage();
        // allow finally block to handle termination
        // } finally {
        // this.unsetRequestSpecificTimeout();
        // Breadcrumbs.clearBreadcrumb();
        // }
        // }
        // } finally {
        // try {
        // this.unsetRequestSpecificTimeout();
        // handleTermination();
        // DistributedSystem.releaseThreadsSockets();
        // } catch (CancelException e) {
        // ignore
        // }
      }

    } catch (CancelException ex) {
    } finally {
      cleanup();
    }
  }

}
