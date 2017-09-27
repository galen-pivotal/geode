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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.security.server.Authenticator;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class HandshakerTest {

  private Map<String, Authenticator> authenticatorMap;
  private Authenticator noopMock;
  private Authenticator simpleMock;
  private Handshaker handshaker;

  @Before
  public void setUp() {
    authenticatorMap = new HashMap<>();

    noopMock = mock(Authenticator.class);
    simpleMock = mock(Authenticator.class);

    authenticatorMap.put("NOOP", noopMock);
    authenticatorMap.put("SIMPLE", simpleMock);

    handshaker = new Handshaker(authenticatorMap);
    assertFalse(handshaker.shaken());
  }

  @Test
  public void version1_0IsSupported() throws Exception {
    HandshakeAPI.HandshakeRequest handshakeRequest = HandshakeAPI.HandshakeRequest.newBuilder()
        .setVersion(HandshakeAPI.Semver.newBuilder().setMajor(1).setMinor(0))
        .setAuthenticationMode(HandshakeAPI.AuthenticationMode.NONE)
        .build();

    ByteArrayInputStream
        byteArrayInputStream =
        ProtobufUtilities.messageToByteArrayInputStream(handshakeRequest);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    Authenticator actualAuthenticator = handshaker
        .handshake(byteArrayInputStream, byteArrayOutputStream);
    assertEquals(noopMock,actualAuthenticator);

    assertTrue(handshaker.shaken());
  }

  @Test(expected = IncompatibleVersionException.class)
  public void version2NotSupported() throws Exception {
    HandshakeAPI.HandshakeRequest handshakeRequest = HandshakeAPI.HandshakeRequest.newBuilder()
        .setVersion(HandshakeAPI.Semver.newBuilder().setMajor(2).setMinor(0))
        .setAuthenticationMode(HandshakeAPI.AuthenticationMode.NONE)
        .build();

    ByteArrayInputStream
        byteArrayInputStream =
        ProtobufUtilities.messageToByteArrayInputStream(handshakeRequest);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    handshaker.handshake(byteArrayInputStream, byteArrayOutputStream);
  }

  @Test(expected = IncompatibleVersionException.class)
  public void bogusAuthenticationMode() throws Exception {
    HandshakeAPI.HandshakeRequest handshakeRequest = HandshakeAPI.HandshakeRequest.newBuilder()
        .setVersion(HandshakeAPI.Semver.newBuilder().setMajor(1).setMinor(0))
        .setAuthenticationModeValue(-1)
        .build();

    ByteArrayInputStream
        byteArrayInputStream =
        ProtobufUtilities.messageToByteArrayInputStream(handshakeRequest);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    handshaker.handshake(byteArrayInputStream, byteArrayOutputStream);
  }

  @Test
  public void simpleIsSupported() throws Exception {
    HandshakeAPI.HandshakeRequest handshakeRequest = HandshakeAPI.HandshakeRequest.newBuilder()
        .setVersion(HandshakeAPI.Semver.newBuilder().setMajor(1).setMinor(0))
        .setAuthenticationMode(HandshakeAPI.AuthenticationMode.SIMPLE)
        .build();

    ByteArrayInputStream
        byteArrayInputStream =
        ProtobufUtilities.messageToByteArrayInputStream(handshakeRequest);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    Authenticator actualAuthenticator = handshaker
        .handshake(byteArrayInputStream, byteArrayOutputStream);
    assertEquals(simpleMock, actualAuthenticator);

    assertTrue(handshaker.shaken());
  }
}
