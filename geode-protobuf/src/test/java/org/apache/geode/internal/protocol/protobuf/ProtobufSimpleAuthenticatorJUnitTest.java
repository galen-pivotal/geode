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

package org.apache.geode.internal.protocol.protobuf;

import org.apache.geode.examples.security.ExampleSecurityManager;
import org.apache.geode.internal.protocol.protobuf.security.ProtobufSimpleAuthenticator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.test.junit.categories.UnitTest;

import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class ProtobufSimpleAuthenticatorJUnitTest {
  private static final String TEST_USERNAME = "user1";
  private static final String TEST_PASSWORD = "hunter2";
  private ByteArrayInputStream byteArrayInputStream; // initialized with an incoming request in
                                                     // setUp.
  private ByteArrayOutputStream byteArrayOutputStream;
  private ProtobufSimpleAuthenticator protobufSimpleAuthenticator;
  private SecurityService mockSecurityService;
  private Subject mockSecuritySubject;
  private Properties expectedAuthProperties;

  @Before
  public void setUp() throws IOException {
    AuthenticationAPI.SimpleAuthenticationRequest basicAuthenticationRequest =
        AuthenticationAPI.SimpleAuthenticationRequest.newBuilder()
            .putCredentials(ResourceConstants.USER_NAME, TEST_USERNAME)
            .putCredentials(ResourceConstants.PASSWORD, TEST_PASSWORD).build();

    expectedAuthProperties = new Properties();
    expectedAuthProperties.setProperty(ResourceConstants.USER_NAME, TEST_USERNAME);
    expectedAuthProperties.setProperty(ResourceConstants.PASSWORD, TEST_PASSWORD);

    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
    basicAuthenticationRequest.writeDelimitedTo(messageStream);
    byteArrayInputStream = new ByteArrayInputStream(messageStream.toByteArray());
    byteArrayOutputStream = new ByteArrayOutputStream();

    mockSecuritySubject = mock(Subject.class);
    mockSecurityService = mock(SecurityService.class);
    when(mockSecurityService.login(expectedAuthProperties)).thenReturn(mockSecuritySubject);

    protobufSimpleAuthenticator = new ProtobufSimpleAuthenticator();
  }

  @Test
  public void successfulAuthentication() throws IOException {
    assertFalse(protobufSimpleAuthenticator.isAuthenticated());

    protobufSimpleAuthenticator.authenticate(byteArrayInputStream, byteArrayOutputStream,
        mockSecurityService);

    AuthenticationAPI.SimpleAuthenticationResponse simpleAuthenticationResponse =
        getSimpleAuthenticationResponse(byteArrayOutputStream);

    assertTrue(simpleAuthenticationResponse.getAuthenticated());
    assertTrue(protobufSimpleAuthenticator.isAuthenticated());
  }

  @Test
  public void authenticationFails() throws IOException {
    assertFalse(protobufSimpleAuthenticator.isAuthenticated());

    Properties expectedAuthProperties = new Properties();
    expectedAuthProperties.setProperty(ResourceConstants.USER_NAME, TEST_USERNAME);
    expectedAuthProperties.setProperty(ResourceConstants.PASSWORD, TEST_PASSWORD);
    when(mockSecurityService.login(expectedAuthProperties))
        .thenThrow(new AuthenticationFailedException("BOOM!"));

    protobufSimpleAuthenticator.authenticate(byteArrayInputStream, byteArrayOutputStream,
        mockSecurityService);

    AuthenticationAPI.SimpleAuthenticationResponse simpleAuthenticationResponse =
        getSimpleAuthenticationResponse(byteArrayOutputStream);

    assertFalse(simpleAuthenticationResponse.getAuthenticated());
    assertFalse(protobufSimpleAuthenticator.isAuthenticated());
  }

  @Test
  public void authenticationSucceedsButAuthorizationFails() {
    fail("unimplemented");
  }

  @Test
  public void testExampleSecurityManager() throws IOException {
    assertFalse(protobufSimpleAuthenticator.isAuthenticated());

    protobufSimpleAuthenticator.authenticate(byteArrayInputStream, byteArrayOutputStream,
        mockSecurityService);

    new ExampleSecurityManager().init(expectedAuthProperties);
    AuthenticationAPI.SimpleAuthenticationResponse simpleAuthenticationResponse =
        getSimpleAuthenticationResponse(byteArrayOutputStream);

    assertTrue(simpleAuthenticationResponse.getAuthenticated());
    assertTrue(protobufSimpleAuthenticator.isAuthenticated());
  }

  @Test
  public void authenticationRequestedWithNoCacheSecurity() throws IOException {
    when(mockSecurityService.isIntegratedSecurity()).thenReturn(false);
    when(mockSecurityService.isClientSecurityRequired()).thenReturn(false);
    when(mockSecurityService.isPeerSecurityRequired()).thenReturn(false);

    assertFalse(protobufSimpleAuthenticator.isAuthenticated());
    protobufSimpleAuthenticator.authenticate(byteArrayInputStream, byteArrayOutputStream,
        mockSecurityService);

    AuthenticationAPI.SimpleAuthenticationResponse simpleAuthenticationResponse =
        getSimpleAuthenticationResponse(byteArrayOutputStream);

    assertFalse(simpleAuthenticationResponse.getAuthenticated());
    assertFalse(protobufSimpleAuthenticator.isAuthenticated());
  }

  private AuthenticationAPI.SimpleAuthenticationResponse getSimpleAuthenticationResponse(
      ByteArrayOutputStream outputStream) throws IOException {
    ByteArrayInputStream responseStream = new ByteArrayInputStream(outputStream.toByteArray());
    return AuthenticationAPI.SimpleAuthenticationResponse.parseDelimitedFrom(responseStream);
  }
}
