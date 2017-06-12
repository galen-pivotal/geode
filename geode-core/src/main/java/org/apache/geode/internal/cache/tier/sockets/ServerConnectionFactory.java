package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.security.SecurityService;

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

    assert (protocolIterator.hasNext());

    newClientProtocol = protocolIterator.next();

    // TODO handle multiple ClientProtocolMessageHandler impls.
    assert (!protocolIterator.hasNext());

    return newClientProtocol;
  }


  public static ServerConnection makeServerConnection(Socket s, InternalCache c, CachedRegionHelper helper,
                                                      CacheServerStats stats, int hsTimeout, int socketBufferSize, String communicationModeStr,
                                                      byte communicationMode, Acceptor acceptor, SecurityService securityService) {
    if (communicationMode == AcceptorImpl.CLIENT_TO_SERVER_NEW_PROTOCOL) {
      return new NewClientServerConnection(s, c, helper, stats, hsTimeout, socketBufferSize,
          communicationModeStr, communicationMode, acceptor, newClientProtocol);
    } else {
      return new LegacyServerConnection(s, c, helper, stats, hsTimeout, socketBufferSize,
          communicationModeStr, communicationMode, acceptor, securityService);
    }

  }

}
