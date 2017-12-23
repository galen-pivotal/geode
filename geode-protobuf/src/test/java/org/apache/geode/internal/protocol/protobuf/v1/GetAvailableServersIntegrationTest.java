package org.apache.geode.internal.protocol.protobuf.v1;

import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.protocol.protobuf.ProtocolVersion;
import org.apache.geode.internal.protocol.statistics.ProtocolClientStatistics;

public class GetAvailableServersIntegrationTest {

  private InternalLocator locator;
  private int locatorPort;

  @Before
  public void setup() throws IOException {
    System.setProperty("geode.feature-protobuf-protocol", "true");
    locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    locator =
        (InternalLocator) Locator.startLocatorAndDS(locatorPort, new File(""), new Properties());
  }

  @After
  public void teardown() {
    final DistributedSystem distributedSystem = locator.getDistributedSystem();
    locator.stop();
  }

  @Test
  public void test() throws Exception {
    try (Socket socket = new Socket("localhost", locatorPort)) {
      final ProtocolVersion.NewConnectionClientVersion clientVersionMessage =
          ProtocolVersion.NewConnectionClientVersion.newBuilder()
              .setMajorVersion(ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
              .setMinorVersion(ProtocolVersion.MinorVersions.CURRENT_MINOR_VERSION_VALUE).build();

      final OutputStream outputStream = socket.getOutputStream();
      final InputStream inputStream = socket.getInputStream();

      clientVersionMessage.writeDelimitedTo(outputStream);

      Awaitility.await().atMost(3, TimeUnit.SECONDS).until(Awaitility
          .matches(() -> ProtocolVersion.VersionAcknowledgement.parseDelimitedFrom(inputStream)));

      ClientProtocol.Message.newBuilder()
          .setRequest(ClientProtocol.Request.newBuilder()
              .setGetAvailableServersRequest(LocatorAPI.GetAvailableServersRequest.newBuilder()))
          .build().writeDelimitedTo(outputStream);

      Awaitility.await().atMost(3, TimeUnit.SECONDS)
          .until(Awaitility.matches(() -> ClientProtocol.Message.parseDelimitedFrom(inputStream)));

    }
  }
}
