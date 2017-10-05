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
package org.apache.geode.internal.protocol;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.protocol.protobuf.AuthenticationAPI;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class AuthenticationIntegrationTest {

  private static final String TEST_USERNAME = "bob";
  private static final String TEST_PASSWORD = "bobspassword";
  private Cache cache;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private OutputStream outputStream;
  private InputStream inputStream;
  private ProtobufProtocolSerializer protobufProtocolSerializer;
  private Properties expectedAuthProperties;
  private Socket socket;

  @Before
  public void setUp() {
    System.setProperty("geode.feature-protobuf-protocol", "true");
  }

  public void setupServerAndStreams() throws IOException {
    CacheServer cacheServer = cache.addCacheServer();
    int cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();

    socket = new Socket("localhost", cacheServerPort);

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    outputStream = socket.getOutputStream();
    inputStream = socket.getInputStream();
    outputStream.write(110);

    protobufProtocolSerializer = new ProtobufProtocolSerializer();
  }

  private static class SimpleSecurityManager implements SecurityManager{
    private final Object authorizedPrincipal;
    private final Properties authorizedCredentials;

    SimpleSecurityManager(Object validPrincipal, Properties validCredentials) {
      this.authorizedPrincipal = validPrincipal;
      authorizedCredentials = validCredentials;
    }

    @Override
    public Object authenticate(Properties credentials) throws AuthenticationFailedException {
      if (authorizedCredentials.equals(credentials)) {
        return authorizedPrincipal;
      } else {
        throw new AuthenticationFailedException("Test properties: " + credentials + " don't match authorized " + authorizedCredentials);
      }
    }

    @Override
    public boolean authorize(Object principal, ResourcePermission permission) {
      return principal == authorizedPrincipal;
    }
  }

  private Cache createCacheWithSecurityManagerTakingExpectedCreds() {
    expectedAuthProperties = new Properties();
    expectedAuthProperties.setProperty(ResourceConstants.USER_NAME, TEST_USERNAME);
    expectedAuthProperties.setProperty(ResourceConstants.PASSWORD, TEST_PASSWORD);

    SimpleSecurityManager
        securityManager =
        new SimpleSecurityManager("this is a secret string or something.", expectedAuthProperties);

    Properties properties = new Properties();
    CacheFactory cacheFactory = new CacheFactory(properties);
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0"); // sometimes it isn't due to other
    // tests.
    cacheFactory.set(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");

    cacheFactory.setSecurityManager(securityManager);
    return cacheFactory.create();
  }

  private Cache createNoSecurityCache() {
    Properties properties = new Properties();
    CacheFactory cacheFactory = new CacheFactory(properties);
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0"); // sometimes it isn't due to other
    // tests.
    cacheFactory.set(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");

    return cacheFactory.create();
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
      cache = null;
    }
  }

  @Test
  public void noopAuthenticationSucceeds() throws Exception {
    cache = createNoSecurityCache();
    setupServerAndStreams();
    ClientProtocol.Message getRegionsMessage =
        ClientProtocol.Message.newBuilder().setRequest(ClientProtocol.Request.newBuilder()
            .setGetRegionNamesRequest(RegionAPI.GetRegionNamesRequest.newBuilder())).build();
    protobufProtocolSerializer.serialize(getRegionsMessage, outputStream);

    ClientProtocol.Message regionsResponse = protobufProtocolSerializer.deserialize(inputStream);
    assertEquals(ClientProtocol.Response.ResponseAPICase.GETREGIONNAMESRESPONSE,
        regionsResponse.getResponse().getResponseAPICase());
  }

  @Test
  public void simpleAuthenticationSucceeds() throws Exception {
    cache = createCacheWithSecurityManagerTakingExpectedCreds();
    setupServerAndStreams();

    assertTrue(((InternalCache) cache).getSecurityService().isIntegratedSecurity());
    assertTrue(((InternalCache) cache).getSecurityService().isClientSecurityRequired());
    assertTrue(((InternalCache) cache).getSecurityService().isPeerSecurityRequired());

    AuthenticationAPI.SimpleAuthenticationRequest authenticationRequest =
        AuthenticationAPI.SimpleAuthenticationRequest.newBuilder()
            .putCredentials(ResourceConstants.USER_NAME, TEST_USERNAME)
            .putCredentials(ResourceConstants.PASSWORD, TEST_PASSWORD).build();
    authenticationRequest.writeDelimitedTo(outputStream);

    AuthenticationAPI.SimpleAuthenticationResponse authenticationResponse =
        AuthenticationAPI.SimpleAuthenticationResponse.parseDelimitedFrom(inputStream);
    assertTrue(authenticationResponse.getAuthenticated());

    ClientProtocol.Message getRegionsMessage =
        ClientProtocol.Message.newBuilder().setRequest(ClientProtocol.Request.newBuilder()
            .setGetRegionNamesRequest(RegionAPI.GetRegionNamesRequest.newBuilder())).build();
    protobufProtocolSerializer.serialize(getRegionsMessage, outputStream);

    ClientProtocol.Message regionsResponse = protobufProtocolSerializer.deserialize(inputStream);
    assertEquals(ClientProtocol.Response.ResponseAPICase.GETREGIONNAMESRESPONSE,
        regionsResponse.getResponse().getResponseAPICase());

  }

  @Test
  public void simpleAuthenticationWithEmptyCreds() throws Exception {
    cache = createCacheWithSecurityManagerTakingExpectedCreds();
    setupServerAndStreams();

    AuthenticationAPI.SimpleAuthenticationRequest authenticationRequest =
        AuthenticationAPI.SimpleAuthenticationRequest.newBuilder().build();

    authenticationRequest.writeDelimitedTo(outputStream);

    AuthenticationAPI.SimpleAuthenticationResponse authenticationResponse =
        AuthenticationAPI.SimpleAuthenticationResponse.parseDelimitedFrom(inputStream);
    assertFalse(authenticationResponse.getAuthenticated());

    try {
      authenticationRequest.writeDelimitedTo(outputStream);
      fail("Should have thrown socket closed exception");
    } catch (SocketException expected) {
      return;
    }
  }

  @Test
  public void simpleAuthenticationWithInvalidCreds() throws Exception {
    cache = createCacheWithSecurityManagerTakingExpectedCreds();
    setupServerAndStreams();

    AuthenticationAPI.SimpleAuthenticationRequest authenticationRequest =
        AuthenticationAPI.SimpleAuthenticationRequest.
            newBuilder().putCredentials(ResourceConstants.USER_NAME, TEST_USERNAME)
        .putCredentials(ResourceConstants.PASSWORD, "wrong password").build();

    authenticationRequest.writeDelimitedTo(outputStream);

    AuthenticationAPI.SimpleAuthenticationResponse authenticationResponse =
        AuthenticationAPI.SimpleAuthenticationResponse.parseDelimitedFrom(inputStream);
    assertFalse(authenticationResponse.getAuthenticated());
    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(socket::isInputShutdown);
    assertEquals("Socket should be closed",-1, inputStream.read());
  }

  @Test
  public void noAuthenticatorSet() throws IOException {
    cache = createNoSecurityCache();
    setupServerAndStreams();

    assertFalse(((InternalCache) cache).getSecurityService().isIntegratedSecurity());
    assertFalse(((InternalCache) cache).getSecurityService().isClientSecurityRequired());
    assertFalse(((InternalCache) cache).getSecurityService().isPeerSecurityRequired());
  }
}
