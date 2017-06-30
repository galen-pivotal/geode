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

import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.ProtobufUtilities;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.operations.OperationHandler;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.protocol.protobuf.EncodingTypeTranslator;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

public class GetRequestOperationHandler implements
    OperationHandler<RegionAPI.GetRequest, RegionAPI.GetResponse, ClientProtocol.ErrorResponse> {

  @Override
  public OperationResponse<RegionAPI.GetResponse, ClientProtocol.ErrorResponse> process(
      SerializationService serializationService, RegionAPI.GetRequest request, Cache cache) {
    String regionName = request.getRegionName();
    try {
      Object decodedKey = ProtobufUtilities.decodeValue(serializationService, request.getKey());

      Region region = cache.getRegion(regionName);

      if (region == null) {
        return OperationResponse
            .Error(ProtobufUtilities.createErrorResponse(false, false, "Region not found"));
      }

      Object resultValue = region.get(decodedKey);

      if (resultValue == null) {
        return OperationResponse.Response(buildGetResponseKeyNotFound());
      }

      return OperationResponse.Response(buildGetResponseSuccess(serializationService, resultValue));
    } catch (UnsupportedEncodingTypeException ex) {
      // can be thrown by encoding or decoding.
      cache.getLogger().error("encoding not supported ", ex);
      return OperationResponse
          .Error(ProtobufUtilities.createErrorResponse(false, false, "encoding not supported "));
    } catch (CodecNotRegisteredForTypeException ex) {
      cache.getLogger().error("codec error in protobuf deserialization ", ex);
      return OperationResponse.Error(ProtobufUtilities.createErrorResponse(true, false,
          "codec error in protobuf deserialization "));
    }
  }

  private RegionAPI.GetResponse buildGetResponseKeyNotFound() {
    return RegionAPI.GetResponse.newBuilder()
        .setNull(RegionAPI.LookupFailure.newBuilder().setKeyInKeySet(false)).build();
  }

  // throws if the object in the cache is not of a class that be serialized via the protobuf
  // protocol.
  private RegionAPI.GetResponse buildGetResponseSuccess(SerializationService serializationService,
      Object resultValue)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    BasicTypes.EncodingType resultEncodingType =
        EncodingTypeTranslator.getEncodingTypeForObject(resultValue);
    byte[] resultEncodedValue = serializationService.encode(resultEncodingType, resultValue);

    return RegionAPI.GetResponse.newBuilder()
        .setResult(ProtobufUtilities.getEncodedValue(resultEncodingType, resultEncodedValue))
        .build();
  }
}

