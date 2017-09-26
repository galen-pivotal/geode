package org.apache.geode.internal.cache.tier.sockets;

/**
 * Provides a convenient location for a client protocol service t
 */
public interface ClientProtocolService {
  ClientProtocolHandshaker getHandshaker();

  ClientProtocolMessageHandler getMessageHandler();


}
