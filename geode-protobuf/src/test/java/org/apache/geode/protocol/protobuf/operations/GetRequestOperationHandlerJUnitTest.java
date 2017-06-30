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
package org.apache.geode.protocol.protobuf.operations;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.MessageUtil;
import org.apache.geode.protocol.operations.OperationHandler;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.Charset;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class GetRequestOperationHandlerJUnitTest {
  public static final String TEST_KEY = "my key";
  public static final String TEST_VALUE = "my value";
  public static final String TEST_REGION = "test region";
  public static final String MISSING_REGION = "missing region";
  public static final String MISSING_KEY = "missing key";
  public Cache cacheStub;
  public SerializationService serializationServiceStub;
  private GetRequestOperationHandler operationHandler;

  @Before
  public void setUp() throws Exception {
    serializationServiceStub = mock(SerializationService.class);
    when(serializationServiceStub.decode(BasicTypes.EncodingType.STRING,
        TEST_KEY.getBytes(Charset.forName("UTF-8")))).thenReturn(TEST_KEY);
    when(serializationServiceStub.encode(BasicTypes.EncodingType.STRING, TEST_VALUE))
        .thenReturn(TEST_VALUE.getBytes(Charset.forName("UTF-8")));

    Region regionStub = mock(Region.class);
    when(regionStub.get(TEST_KEY)).thenReturn(TEST_VALUE);

    cacheStub = mock(Cache.class);
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionStub);
    when(cacheStub.getRegion(MISSING_REGION)).thenReturn(null);
    operationHandler = new GetRequestOperationHandler();
  }

  @Test
  public void processReturnsTheEncodedValueFromTheRegion()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException {
    OperationHandler.OperationResponse<RegionAPI.GetResponse, ClientProtocol.ErrorResponse> response =
        operationHandler.process(serializationServiceStub,
            MessageUtil.makeGetRequest(TEST_KEY, TEST_REGION), cacheStub);

    Assert.assertFalse(response.isError);
    Assert.assertEquals(BasicTypes.EncodingType.STRING,
        response.response.getResult().getEncodingType());
    String actualValue =
        MessageUtil.getStringCodec().decode(response.response.getResult().getValue().toByteArray());
    Assert.assertEquals(TEST_VALUE, actualValue);
  }

  @Test
  public void processReturnsUnsucessfulResponseForInvalidRegion()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException {
    GetRequestOperationHandler.OperationResponse response = operationHandler.process(
        serializationServiceStub, MessageUtil.makeGetRequest(TEST_KEY, MISSING_REGION), cacheStub);

    assertTrue(response.isError);
    Assert.assertNull(response.response);
    Assert.assertNotNull(response.error);
  }

  @Test
  public void processReturnsKeyNotFoundWhenKeyIsNotFound()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException {
    OperationHandler.OperationResponse<RegionAPI.GetResponse, ClientProtocol.ErrorResponse> response =
        operationHandler.process(serializationServiceStub,
            MessageUtil.makeGetRequest(MISSING_KEY, TEST_REGION), cacheStub);

    Assert.assertFalse(response.isError);
    Assert.assertNotNull(response.response);
    Assert.assertNull(response.error);
    Assert.assertNotNull(response.response.getNull());
    Assert.assertFalse(response.response.getNull().getKeyInKeySet());
  }

  @Test
  public void processReturnsErrorWhenUnableToDecodeRequest()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException {
    LogWriter loggerMock = mock(LogWriter.class);
    when(cacheStub.getLogger()).thenReturn(loggerMock);

    CodecNotRegisteredForTypeException exception =
        new CodecNotRegisteredForTypeException("error finding codec for type");
    when(serializationServiceStub.decode(BasicTypes.EncodingType.STRING,
        TEST_KEY.getBytes(Charset.forName("UTF-8")))).thenThrow(exception);

    OperationHandler.OperationResponse<RegionAPI.GetResponse, ClientProtocol.ErrorResponse> response =
        operationHandler.process(serializationServiceStub,
            MessageUtil.makeGetRequest(TEST_KEY, TEST_REGION), cacheStub);

    Assert.assertFalse(response.isError);
    verify(loggerMock).error(any(String.class), eq(exception));
  }
}
