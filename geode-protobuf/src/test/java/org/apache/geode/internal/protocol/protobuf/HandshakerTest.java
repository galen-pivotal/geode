package org.apache.geode.internal.protocol.protobuf;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.server.Authenticator;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class HandshakerTest {
  @Test
  public void test() throws IOException {
    byte[] bytes = new byte[1024];
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    Map<String, Authenticator> authenticatorMap = new HashMap<>();

    Authenticator noopMock = mock(Authenticator.class);
    Authenticator simpleMock = mock(Authenticator.class);

    authenticatorMap.put("NOOP", noopMock);
    authenticatorMap.put("SIMPLE", simpleMock);

    Handshaker handshaker = new Handshaker(authenticatorMap);

    assertFalse(handshaker.shaken());

    handshaker.handshake(byteArrayInputStream, byteArrayOutputStream);

    assertTrue(handshaker.shaken());

    verify(noopMock).getAuthorizer();
    verify(noopMock).authenticate(any(), any(), any());
  }
}
