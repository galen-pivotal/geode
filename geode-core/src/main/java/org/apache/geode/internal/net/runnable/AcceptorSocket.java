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

  public AcceptorSocket(AcceptorImpl acceptor, Logger logger) {
    super(acceptor, logger);
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
