package org.apache.geode.protocol;

import static org.apache.geode.distributed.ConfigurationProperties.CONFLATE_EVENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.junit.Assert.assertEquals;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.protocol.protobuf.AuthenticationAPI;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.protobuf.ProtobufSerializationService;
import org.apache.geode.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.util.test.TestUtil;

@Category(DistributedTest.class)
public class ProtobufPerformanceDUnitTest extends JUnit4CacheTestCase {

  private final String DEFAULT_STORE = "default.keystore";
  private final String SSL_PROTOCOLS = "any";
  private final String SSL_CIPHERS = "any";

  private Cache cache;
  private final String TEST_REGION = "testRegion";
  private Socket socket;
  private OutputStream outputStream;
  private ProtobufSerializationService serializationService;
  private int cacheServerPortNetty = 40406;
  private int cacheServerPortOld = 40404;

  @Test
  public void perfTest() throws Exception {

    Host host = Host.getHost(0);
    VM server1 = host.getVM(2);

    server1.invoke(() -> startCache());
    serializationService = new ProtobufSerializationService();

    ArrayList<Socket> connections = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      connections.add(initConnection(false));
    }

    long startTime = System.currentTimeMillis();

    List<Object> objectList = connections.parallelStream().map((sock) -> {
      int nputs = 500;
      byte[] value = new byte[100];
      for (int i = 0; i < nputs; i++) {
        try {
          makePut(i, value, sock);
        } catch (Exception e) {
          System.out.println("===> Oh no!" + e);
        }
      }
      return null;
    }).collect(Collectors.toList());
    System.out.println("===>" + objectList.size());
//    int nputs = 1_000_000;

    long endTime = System.currentTimeMillis();

    System.out.println("===> perf time " + (endTime - startTime));
  }

  private void makePut(int key, byte[] value, Socket socket)
      throws Exception {
    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message putMessage =
        MessageUtil
            .makePutRequestMessage(serializationService, String.valueOf(key), String.valueOf(value),
                TEST_REGION,
                ProtobufUtilities.createMessageHeader(2));
    protobufProtocolSerializer.serialize(putMessage, socket.getOutputStream());
    validatePutResponse(socket, protobufProtocolSerializer);
  }

  private void validatePutResponse(Socket socket,
                                   ProtobufProtocolSerializer protobufProtocolSerializer)
      throws Exception {
    ClientProtocol.Response response =
        deserializeResponse(socket, protobufProtocolSerializer, 2);
    assertEquals(ClientProtocol.Response.ResponseAPICase.PUTRESPONSE,
        response.getResponseAPICase());
  }

  private ClientProtocol.Response deserializeResponse(Socket socket,
                                                      ProtobufProtocolSerializer protobufProtocolSerializer,
                                                      int expectedCorrelationId)
      throws InvalidProtocolMessageException, IOException {
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(expectedCorrelationId, message.getMessageHeader().getCorrelationId());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE, message.getMessageTypeCase());
    return message.getResponse();
  }

  private void startCache() throws Exception {
    System.setProperty("nettyPort", String.valueOf(cacheServerPortNetty));
    Properties properties = new Properties();
    updatePropertiesForSSLCache(properties);

    CacheFactory cacheFactory = new CacheFactory(properties);
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0");
    cacheFactory.set(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.SECURITY_MANAGER,
        "org.apache.geode.security.SimpleTestSecurityManager");
    cache = cacheFactory.create();

    CacheServer cacheServer = cache.addCacheServer();
    //cacheServerPortNetty = AvailablePortHelper.getRandomAvailableTCPPort();
    // HACK
    cacheServerPortNetty = 40405;
    cacheServer.setPort(cacheServerPortOld);
    cacheServer.start();

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.create(TEST_REGION);

    System.setProperty("geode.feature-protobuf-protocol", "true");

    System.out.println("port 1: " + cacheServerPortNetty);
//    nettyServer = new NettyServer(cacheServerPortNetty, cache);
//    nettyServer.run();

  }

  private Socket initConnection(boolean useSSL) throws Exception {
    Socket socket;
    if (useSSL) {
      socket = getSSLSocket(cacheServerPortNetty);
    } else {
      socket = new Socket("localhost", cacheServerPortNetty);

    }
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
//    outputStream.write(110);

    AuthenticationAPI.SimpleAuthenticationRequest.newBuilder().setUsername("admin")
        .setPassword("admin").build().writeDelimitedTo(socket.getOutputStream());

    AuthenticationAPI.SimpleAuthenticationResponse.parseDelimitedFrom(socket.getInputStream());

    return socket;
  }

  private Socket getSSLSocket(int port) throws IOException {
    String keyStorePath =
        TestUtil.getResourcePath(RoundTripCacheConnectionJUnitTest.class, DEFAULT_STORE);
    String trustStorePath =
        TestUtil.getResourcePath(RoundTripCacheConnectionJUnitTest.class, DEFAULT_STORE);

    SSLConfig sslConfig = new SSLConfig();
    sslConfig.setEnabled(true);
    sslConfig.setCiphers(SSL_CIPHERS);
    sslConfig.setProtocols(SSL_PROTOCOLS);
    sslConfig.setRequireAuth(true);
    sslConfig.setKeystoreType("jks");
    sslConfig.setKeystore(keyStorePath);
    sslConfig.setKeystorePassword("password");
    sslConfig.setTruststore(trustStorePath);
    sslConfig.setKeystorePassword("password");

    SocketCreator socketCreator = new SocketCreator(sslConfig);
    return socketCreator.connectForClient("localhost", port, 5000);
  }

  private void updatePropertiesForSSLCache(Properties properties) {
    String keyStore =
        TestUtil.getResourcePath(RoundTripCacheConnectionJUnitTest.class, DEFAULT_STORE);
    String trustStore =
        TestUtil.getResourcePath(RoundTripCacheConnectionJUnitTest.class, DEFAULT_STORE);

    properties.put(SSL_ENABLED_COMPONENTS, "server");
    properties.put(ConfigurationProperties.SSL_PROTOCOLS, SSL_PROTOCOLS);
    properties.put(ConfigurationProperties.SSL_CIPHERS, SSL_CIPHERS);
    properties.put(SSL_REQUIRE_AUTHENTICATION, String.valueOf(true));

    properties.put(SSL_KEYSTORE_TYPE, "jks");
    properties.put(SSL_KEYSTORE, keyStore);
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_TRUSTSTORE, trustStore);
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
  }
}
