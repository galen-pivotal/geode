package org.apache.geode.internal.cache.tier.sockets;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.geode.security.server.Authenticator;

public interface ClientProtocolHandshaker {
  public Authenticator handshake(InputStream inputStream, OutputStream outputStream)
      throws IOException;

  public boolean shaken();
}
