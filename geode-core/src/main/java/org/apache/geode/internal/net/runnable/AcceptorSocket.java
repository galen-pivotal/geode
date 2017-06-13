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

package org.apache.geode.internal.net.runnable;

import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

/**
 * Should allow us to hook the new client protocol in.
 */
public abstract class AcceptorSocket extends AcceptorConnection {
  protected Socket socket;

  public AcceptorSocket(AcceptorImpl acceptor, Logger logger, Socket socket) {
    super(acceptor, logger);
    this.socket = socket;
  }

  public void registerWithSelector() throws IOException {
    // logger.info("DEBUG: registerWithSelector " + this);
    getSelectableChannel().configureBlocking(false);
    this.acceptor.registerSC(this);
  }

  public SelectableChannel getSelectableChannel() {
    return this.socket.getChannel();
  }

  public void registerWithSelector2(Selector s) throws IOException {
    /* this.sKey = */
    getSelectableChannel().register(s, SelectionKey.OP_READ, this);
  }

}
