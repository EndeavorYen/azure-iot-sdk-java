package com.microsoft.azure.sdk.iot.device;

import com.microsoft.azure.sdk.iot.device.transport.amqps.IoTHubConnectionType;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A client for creating multiplexed connections to IoT Hub. A multiplexed connection allows for multiple device clients
 * to communicate to the service through a single AMQPS connection.
 * <p>
 * A given AMQPS connection requires a TLS connection, so multiplexing may be worthwhile if you want to limit the number
 * of TLS connections needed to connect multiple device clients to IoT Hub.
 * <p>
 * A given multiplexing client also has a fixed amount of worker threads regardless of how many device clients are
 * being multiplexed. Comparatively, every non-multiplexed device client instance has its own set of worker
 * threads. Multiplexing may be worthwhile if you want fewer worker threads.
 * <p>
 * Only AMQPS and AMQPS_WS support multiplexing, and only symmetric key authenticated devices can be multiplexed.
 * <p>
 * {@link ModuleClient} instances cannot be multiplexed.
 */
@Slf4j
public class MultiplexingClient
{
    public static long SEND_PERIOD_MILLIS = 10L;
    public static long RECEIVE_PERIOD_MILLIS = 10L;

    private DeviceIO deviceIO;
    private ArrayList<DeviceClient> deviceClientList;
    private IotHubClientProtocol protocol;
    private ProxySettings proxySettings;
    private final Object lock = new Object();

    // Connection status change callbacks for the multiplexed connection
    private IotHubConnectionStatusChangeCallback connectionStatusChangeCallback;
    private Object connectionStatusChangeCallbackContext;

    /**
     * The maximum number of devices that can be multiplexed together on a single multiplexed AMQPS connection
     */
    public static final int MAX_MULTIPLEX_DEVICE_COUNT_AMQPS = 1000;

    /**
     * The maximum number of devices that can be multiplexed together on a single multiplexed AMQPS_WS connection
     */
    public static final int MAX_MULTIPLEX_DEVICE_COUNT_AMQPS_WS = 500;

    /**
     * Instantiate a new MultiplexingClient that will establish a multiplexed connection through a proxy.
     *
     * @param protocol The transport protocol that this client will build the multiplexed connection on. Must be either
     *                 {@link IotHubClientProtocol#AMQPS} or {@link IotHubClientProtocol#AMQPS_WS}.
     */
    public MultiplexingClient(IotHubClientProtocol protocol)
    {
        this(protocol, null);
    }

    /**
     * Instantiate a new MultiplexingClient that will establish a multiplexed connection through a proxy.
     *
     * @param protocol The transport protocol that this client will build the multiplexed connection on. Must be
     * {@link IotHubClientProtocol#AMQPS_WS} since using {@link IotHubClientProtocol#AMQPS} does not support proxies.
     * @param proxySettings The proxy settings that this client will use.
     */
    public MultiplexingClient(IotHubClientProtocol protocol, ProxySettings proxySettings)
    {
        switch (protocol)
        {
            case AMQPS:
            case AMQPS_WS:
                break;
            default:
                throw new IllegalArgumentException("Multiplexing is only supported for AMQPS and AMQPS_WS");
        }

        this.deviceClientList = new ArrayList<>();
        this.protocol = protocol;
        this.proxySettings = proxySettings;
    }

    /**
     * Opens this multiplexing client. At least one device client must be registered prior to calling this.
     * <p>
     * This call behaves synchronously, so if it returns without throwing, then all registered device clients were
     * successfully opened.
     * <p>
     * If this client is already open, then this method will do nothing.
     * <p>
     * @throws IOException If any IO errors occur while opening the multiplexed connection.
     */
    public void open() throws IOException
    {
        synchronized (this.lock)
        {
            if (deviceClientList.size() < 1)
            {
                throw new IllegalStateException("Must register at least one device client before opening a multiplexed connection");
            }

            log.info("Opening multiplexing client");
            this.deviceIO.open();
            log.info("Successfully opened multiplexing client");
        }
    }

    /**
     * Close this multiplexing client. This will close all active device sessions as well as the AMQP connection.
     * <p>
     * If this client is already closed, then this method will do nothing.
     * <p>
     * Once closed, this client can be re-opened. It will preserve the previously registered device clients.
     * <p>
     * @throws IOException If any exception occurs while closing the connection.
     */
    public void close() throws IOException
    {
        synchronized (this.lock)
        {
            log.info("Closing multiplexing client");
            for (DeviceClient deviceClient : this.deviceClientList)
            {
                deviceClient.closeFileUpload();
            }

            if (this.deviceIO != null)
            {
                this.deviceIO.multiplexClose();
            }

            // Note that this method does not close each of the registered device client instances. This is intentional
            // as the calls to deviceClient.close() do nothing besides close the deviceIO layer, which is already closed
            // by the above code.

            log.info("Successfully closed multiplexing client");
        }
    }

    /**
     * Add a device to this multiplexing client. This method may be called before or after opening the multiplexed
     * connection, but will behave differently depending on when it was called.
     * <p>
     * Up to 1000 devices can be registered on a multiplexed AMQPS connection, and up to 500 devices can be registered on a
     * multiplexed AMQPS_WS connection.
     * <p>
     * If the multiplexing client is already open, then this device client will automatically
     * be opened, too. If the multiplexing client is not open yet, then this device client will not be opened until
     * {@link MultiplexingClient#open()} is called.
     * <p>
     * If the multiplexed connection is already open, then this is an asynchronous operation. You can track the state of your
     * device session using the {@link DeviceClient#registerConnectionStatusChangeCallback(IotHubConnectionStatusChangeCallback, Object)}.
     * That callback will execute once your device session has successfully been added to the existing multiplexed connection and
     * is ready to send telemetry.
     * <p>
     * Any proxy settings set to the provided device client will be overwritten by the proxy settings of this multiplexing client.
     * <p>
     * The registered device client instance must use the same transport protocol (AMQPS or AMQPS_WS) that this multiplexing client uses.
     * <p>
     * The registered device client may have its own retry policy and its own SAS token expiry time, separate from every other multiplexed device.
     * <p>
     * The registered device client must use symmetric key based authentication.
     * <p>
     * The registered device client must belong to the same IoT Hub as all previously registered device clients.
     * <p>
     * If this device client is already registered, then this method will throw a {@link IllegalStateException}
     * <p>
     * @throws IOException if any exception is thrown when closing the provided device client's old transport layer.
     * @param deviceClient The device client to associate with this multiplexing client.
     */
    public void registerDeviceClient(DeviceClient deviceClient, boolean blocking) {
        Objects.requireNonNull(deviceClient);

        synchronized (this.lock)
        {
            for (DeviceClient currentClient : deviceClientList)
            {
                String currentDeviceId = currentClient.getConfig().getDeviceId();
                if (currentDeviceId.equalsIgnoreCase(deviceClient.getConfig().getDeviceId()))
                {
                    log.debug("Device {} wasn't registered to the multiplexed connection because it is already registered.", currentDeviceId);
                    return;
                }
            }

            if (deviceClient.getConfig().getAuthenticationType() != DeviceClientConfig.AuthType.SAS_TOKEN)
            {
                throw new UnsupportedOperationException("Can only register to multiplex a device client that uses SAS token based authentication");
            }

            if (deviceClient.getConfig().getProtocol() != this.protocol)
            {
                throw new UnsupportedOperationException("A device client cannot be registered to a multiplexing client that specifies a different transport protocol.");
            }

            if (this.protocol == IotHubClientProtocol.AMQPS && this.deviceClientList.size() > MAX_MULTIPLEX_DEVICE_COUNT_AMQPS)
            {
                throw new UnsupportedOperationException(String.format("Multiplexed connections over AMQPS only support up to %d devices", MAX_MULTIPLEX_DEVICE_COUNT_AMQPS));
            }

            // Typically client side validation is duplicate work, but IoT Hub doesn't give a good error message when closing the
            // AMQPS_WS connection so this is the only way that users will know about this limit
            if (this.protocol == IotHubClientProtocol.AMQPS_WS && this.deviceClientList.size() > MAX_MULTIPLEX_DEVICE_COUNT_AMQPS_WS)
            {
                throw new UnsupportedOperationException(String.format("Multiplexed connections over AMQPS_WS only support up to %d devices", MAX_MULTIPLEX_DEVICE_COUNT_AMQPS_WS));
            }

            if (this.deviceClientList.size() > 1 && !this.deviceClientList.get(0).getConfig().getIotHubHostname().equalsIgnoreCase(deviceClient.getConfig().getIotHubHostname()))
            {
                throw new UnsupportedOperationException("A device client cannot be registered to a multiplexing client that specifies a different host name.");
            }

            // The first device to be registered will cause this client to build the IO layer with its configuration
            if (this.deviceIO == null)
            {
                log.debug("Creating DeviceIO layer for multiplexing client since this is the first registered device");
                this.deviceIO = new DeviceIO(deviceClient.getConfig(), SEND_PERIOD_MILLIS, RECEIVE_PERIOD_MILLIS);

                this.deviceIO.registerMultiplexingConnectionStateCallback(this.connectionStatusChangeCallback, this.connectionStatusChangeCallbackContext);
            }

            if (deviceClient.getDeviceIO() != null && deviceClient.getDeviceIO().isOpen())
            {
                throw new IllegalStateException("Cannot register a device client to a multiplexed connection when it the device client is already open.");
            }

            deviceClient.setAsMultiplexed();
            deviceClient.setDeviceIO(this.deviceIO);
            deviceClient.getConfig().setProxy(this.proxySettings);
            deviceClient.setConnectionType(IoTHubConnectionType.USE_MULTIPLEXING_CLIENT);
            this.deviceClientList.add(deviceClient);

            // if the device IO hasn't been created yet, then this client will be registered once it is created.
            log.info("Registering device {} to multiplexing client", deviceClient.getConfig().getDeviceId());
            this.deviceIO.registerMultiplexedDeviceClient(deviceClient.getConfig(), blocking);
        }
    }

    /**
     * Remove a device client from this multiplexing client. This method may be called before or after opening the
     * multiplexed connection, but will behave differently depending on when it was called.
     * <p>
     * If the multiplexed connection is already open, then this call will close the AMQP device session associated with
     * this device, but it will not close any other registered device sessions or the multiplexing client itself.
     * <p>
     * If the multiplexed connection is already open, then this is an asynchronous operation and you can track the state of your
     * device session using the {@link DeviceClient#registerConnectionStatusChangeCallback(IotHubConnectionStatusChangeCallback, Object)}.
     * <p>
     * If the multiplexed connection is already open, then at least one device client must be registered at any given time.
     * Because of this, this method will throw an {@link IllegalStateException} if it attempts to remove the last device client.
     * <p>
     * Once a device client is unregistered, it may be re-registered to this or any other multiplexing client. It cannot
     * be used in non-multiplexing scenarios though.
     * <p>
     * @param deviceClient The device client to unregister from this multiplexing client.
     */
    public void unregisterDeviceClient(DeviceClient deviceClient)
    {
        Objects.requireNonNull(deviceClient);

        synchronized (this.lock)
        {
            if (deviceClientList.size() <= 1)
            {
                throw new IllegalStateException("Cannot unregister the last device. At least one device client must be registered to this multiplexing client.");
            }

            log.info("Unregistering device {} from multiplexing client", deviceClient.getConfig().getDeviceId());
            this.deviceClientList.remove(deviceClient);
            this.deviceIO.unregisterMultiplexedDeviceClient(deviceClient.getConfig());
            deviceClient.setDeviceIO(null);
        }
    }

    /**
     * Registers a callback to be executed when the connection status of the multiplexed connection as a whole changes.
     * The callback will be fired with a status and a reason why the multiplexed connection's status changed. When the
     * callback is fired, the provided context will be provided alongside the status and reason.
     *
     * <p>Note that this callback will not be fired for device specific connection status changes. In order to be notified
     * when a particular device's connection status changes, you will need to register a connection status change callback
     * on that device client instance using {@link DeviceClient#registerConnectionStatusChangeCallback(IotHubConnectionStatusChangeCallback, Object)}.
     *
     * <p>Note that the thread used to deliver this callback should not be used to call open()/closeNow() on the client
     * that this callback belongs to. All open()/closeNow() operations should be done on a separate thread</p>
     *
     * @param callback The callback to be fired when the connection status of the multiplexed connection changes.
     *                 Can be null to unset this listener as long as the provided callbackContext is also null.
     * @param callbackContext a context to be passed to the callback. Can be {@code null}.
     */
    public void registerConnectionStatusChangeCallback(IotHubConnectionStatusChangeCallback callback, Object callbackContext)
    {
        this.connectionStatusChangeCallback = callback;
        this.connectionStatusChangeCallbackContext = callbackContext;

        if (this.deviceIO != null)
        {
            this.deviceIO.registerMultiplexingConnectionStateCallback(callback, callbackContext);
        }
    }

    /**
     * Returns if the provided device client is already registered to this multiplexing client.
     * @param deviceClient The device client to look for.
     * @return True if the provided device client is already registered to this multiplexing client. False otherwise.
     */
    public boolean isDeviceRegistered(DeviceClient deviceClient)
    {
        return this.deviceClientList.contains(deviceClient);
    }

    /**
     * Get the number of currently registered devices on this multiplexing client.
     * @return The number of currently registered devices on this multiplexing client.
     */
    public int getRegisteredDeviceCount()
    {
        // O(1) operation since ArrayList saves this value as an integer rather than iterating over each.
        // So there is no need to be more clever about this.
        return this.deviceClientList.size();
    }
}
