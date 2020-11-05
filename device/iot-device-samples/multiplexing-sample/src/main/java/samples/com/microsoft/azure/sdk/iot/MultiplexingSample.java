// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package samples.com.microsoft.azure.sdk.iot;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Sample that demonstrates creating a multiplexed connection to IoT Hub using AMQPS / AMQPS_WS. It also demonstrates
 * removing and adding device clients from the multiplexed connection while it is open.
 */
public class MultiplexingSample
{
    // Every multiplexed device will maintain its own connection status callback. Because of that, you can monitor
    // if a particular device session goes offline unexpectedly. This connection status callback is also how you
    // confirm when a device client is connected after registering it to an active multiplexed connection since the .registerDeviceClient(...)
    // call behaves asynchronously when the multiplexing client is already open. Similarly, this callback is used to track
    // when a device client is closed when unregistering it from an active connection.
    public static class MultiplexedDeviceConnectionStatusChangeTracker implements IotHubConnectionStatusChangeCallback
    {
        private boolean isOpen = false;
        private boolean isDisconnectedRetrying = false;

        public boolean isOpen() {
            return isOpen;
        }

        public boolean isDisconnectedRetrying() {
            return isDisconnectedRetrying;
        }

        @Override
        public void execute(IotHubConnectionStatus status, IotHubConnectionStatusChangeReason statusChangeReason, Throwable throwable, Object callbackContext)
        {
            String deviceId = (String) callbackContext;

            if (throwable == null)
            {
                System.out.println("CONNECTION STATUS UPDATE FOR DEVICE " + deviceId + " - " + status + ", " + statusChangeReason);
            }
            else
            {
                System.out.println("CONNECTION STATUS UPDATE FOR DEVICE " + deviceId + " - " + status + ", " + statusChangeReason + ", " + throwable.getMessage());
            }

            if (status == IotHubConnectionStatus.CONNECTED)
            {
                isOpen = true;
                isDisconnectedRetrying = false;
            }
            else if (status == IotHubConnectionStatus.DISCONNECTED)
            {
                isOpen = false;
                isDisconnectedRetrying = false;
            }
            else if (status == IotHubConnectionStatus.DISCONNECTED_RETRYING)
            {
                isDisconnectedRetrying = true;
                isOpen = false;
            }
        }
    }

    public static int acknowledgedSentMessages = 0;
    protected static class TelemetryAcknowledgedEventCallback implements IotHubEventCallback
    {
        public void execute(IotHubStatusCode status, Object context)
        {
            String messageId = (String) context;
            System.out.println("IoT Hub responded to message "+ messageId  + " with status " + status.name());
            acknowledgedSentMessages++;
        }
    }

    private static final int MULTIPLEXED_DEVICE_CLIENT_COUNT = 3;

    /**
     * Multiplex devices an IoT Hub using AMQPS / AMQPS_WS
     *
     * @param args
     * args[0] = IoT Hub connection string - Device Client 1
     * args[1] = IoT Hub connection string - Device Client 2
     * args[2] = IoT Hub connection string - Device Client 3
     */
    public static void main(String[] args)
            throws IOException, URISyntaxException, InterruptedException {
        System.out.println("Starting...");
        System.out.println("Beginning setup.");

        if (args.length != 4)
        {
            System.out.format(
                    "Expected 3 arguments but received: %d.\n"
                            + "The program should be called with the following args: \n"
                            + "1. [Device 1 connection string] - String containing Hostname, Device Id & Device Key in one of the following formats: HostName=<iothub_host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>\n"
                            + "2. [Device 2 connection string] - String containing Hostname, Device Id & Device Key in one of the following formats: HostName=<iothub_host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>\n"
                            + "3. [Device 3 connection string] - String containing Hostname, Device Id & Device Key in one of the following formats: HostName=<iothub_host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>\n"
                            + "4. [Protocol]                   - amqps | amqps_ws\n",
                    args.length);
            return;
        }

        String connString1 = args[0];
        String connString2 = args[1];
        String connString3 = args[2];
        IotHubClientProtocol protocol = IotHubClientProtocol.AMQPS;
        if (args[3].equalsIgnoreCase("amqps_ws"))
        {
            protocol = IotHubClientProtocol.AMQPS_WS;
        }

        MultiplexingClient multiplexingClient = new MultiplexingClient(protocol);
        DeviceClient[] multiplexedDeviceClients = new DeviceClient[MULTIPLEXED_DEVICE_CLIENT_COUNT];
        MultiplexedDeviceConnectionStatusChangeTracker[] connectionStatusTrackers = new MultiplexedDeviceConnectionStatusChangeTracker[MULTIPLEXED_DEVICE_CLIENT_COUNT];

        for (int i = 0; i < MULTIPLEXED_DEVICE_CLIENT_COUNT; i++)
        {
            multiplexedDeviceClients[i] = new DeviceClient(args[i], protocol);
            String deviceId = multiplexedDeviceClients[i].getConfig().getDeviceId();
            connectionStatusTrackers[i] = new MultiplexedDeviceConnectionStatusChangeTracker();
            multiplexedDeviceClients[i].registerConnectionStatusChangeCallback(connectionStatusTrackers[i], deviceId);
            multiplexingClient.registerDeviceClient(multiplexedDeviceClients[i], false);
        }

        System.out.println("Opening multiplexed connection");
        // All previously registered device clients will be opened alongside this multiplexing client
        multiplexingClient.open();
        System.out.println("Multiplexed connection opened successfully");

        for (int i = 0; i < MULTIPLEXED_DEVICE_CLIENT_COUNT; i++)
        {
            Message message = new Message("some payload");
            multiplexedDeviceClients[i].sendEventAsync(message, new TelemetryAcknowledgedEventCallback(), message.getMessageId());
        }

        System.out.println("Waiting while messages get sent asynchronously...");
        while (acknowledgedSentMessages < MULTIPLEXED_DEVICE_CLIENT_COUNT)
        {
            System.out.printf(
                    "Waiting on %d messages to be acknowledged out of %d%n",
                    MULTIPLEXED_DEVICE_CLIENT_COUNT - acknowledgedSentMessages,
                    MULTIPLEXED_DEVICE_CLIENT_COUNT);
            Thread.sleep(200);
        }

        // This code demonstrates how to remove a device from an active multiplexed connection without shutting down
        // the whole multiplexed connection or any of the other devices.
        int deviceIndexToUnregister = 0;
        String deviceIdToUnregister = multiplexedDeviceClients[deviceIndexToUnregister].getConfig().getDeviceId();
        System.out.println("Removing device " + deviceIdToUnregister + " from multiplexed connection...");
        // Since this unregisterDeviceClient call is made during an active connection, it will behave asynchronously. Need to check
        // the connection status tracker periodically until it reports that the device has successfully been unregistered.
        multiplexingClient.unregisterDeviceClient(multiplexedDeviceClients[deviceIndexToUnregister]);

        System.out.println("Waiting for device " + deviceIdToUnregister + " to close its device session gracefully...");
        while (connectionStatusTrackers[deviceIndexToUnregister].isOpen())
        {
            Thread.sleep(200);
        }

        System.out.println("Successfully unregistered device " + deviceIdToUnregister + " from an active multiplexed connection.");

        // This code demonstrates how to add a device to an active multiplexed connection without shutting down
        // the whole multiplexed connection or any of the other devices.
        System.out.println("Re-registering device " + deviceIdToUnregister + " to an active multiplexed connection...");
        multiplexingClient.registerDeviceClient(multiplexedDeviceClients[deviceIndexToUnregister], false);

        System.out.println("Waiting for device " + deviceIdToUnregister + " to open its device session on an active multiplexed connection...");
        while (!connectionStatusTrackers[deviceIndexToUnregister].isOpen())
        {
            Thread.sleep(200);
        }

        System.out.println("Successfully registered " + deviceIdToUnregister + " to an active multiplexed connection");

        System.out.println("Closing entire multiplexed connection...");
        // This call will close all multiplexed device client instances as well
        multiplexingClient.close();
        System.out.println("Successfully closed the multiplexed connection");
    }
}
