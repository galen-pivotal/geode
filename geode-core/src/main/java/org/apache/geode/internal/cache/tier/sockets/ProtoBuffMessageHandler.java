package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.tier.Acceptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by gosullivan on 6/7/17.
 */
public class ProtoBuffMessageHandler {
  private void doOneMessage() {
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
}
