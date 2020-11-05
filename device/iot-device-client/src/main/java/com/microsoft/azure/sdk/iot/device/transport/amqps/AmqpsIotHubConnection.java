/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.sdk.iot.device.transport.amqps;

import com.microsoft.azure.proton.transport.proxy.ProxyAuthenticationType;
import com.microsoft.azure.proton.transport.proxy.ProxyConfiguration;
import com.microsoft.azure.proton.transport.proxy.ProxyHandler;
import com.microsoft.azure.proton.transport.proxy.impl.ProxyHandlerImpl;
import com.microsoft.azure.proton.transport.proxy.impl.ProxyImpl;
import com.microsoft.azure.proton.transport.ws.impl.WebSocketImpl;
import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.exceptions.ProtocolException;
import com.microsoft.azure.sdk.iot.device.exceptions.TransportException;
import com.microsoft.azure.sdk.iot.device.transport.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.engine.impl.TransportInternal;
import org.apache.qpid.proton.reactor.Handshaker;
import org.apache.qpid.proton.reactor.Reactor;
import org.apache.qpid.proton.reactor.ReactorOptions;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * An AMQPS IotHub connection between a device and an IoTHub or Edgehub. This class is responsible for reacting to connection level and
 * reactor level events. It is also responsible for creating sessions and handlers for those sessions. An instance of this
 * object may be reused after it has been closed.
 */
@Slf4j
public final class AmqpsIotHubConnection extends BaseHandler implements IotHubTransportConnection, AmqpsSessionStateCallback
{
    // Timeouts
    private static final int MAX_WAIT_TO_CLOSE_CONNECTION = 20 * 1000; // 20 second timeout
    private static final int MAX_WAIT_TO_TERMINATE_EXECUTOR = 10;

    // Web socket constants
    private static final String WEB_SOCKET_PATH = "/$iothub/websocket";
    private static final String WEB_SOCKET_SUB_PROTOCOL = "AMQPWSB10";
    private static final String WEB_SOCKET_QUERY = "iothub-no-client-cert=true";
    private static final int MAX_MESSAGE_PAYLOAD_SIZE = 256 * 1024; //max IoT Hub message size is 256 kb, so amqp websocket layer should buffer at most that much space
    private static final int WEB_SOCKET_PORT = 443;

    private static final int AMQP_PORT = 5671;
    private static final int REACTOR_COUNT = 1;
    private static final int CBS_SESSION_COUNT = 1; //even for multiplex scenarios

    // Message send constants
    private static final int SEND_MESSAGES_PERIOD_MILLIS = 50; //every 50 milliseconds, the method onTimerTask will fire to send, at most, MAX_MESSAGES_TO_SEND_PER_CALLBACK queued messages
    private static final int MAX_MESSAGES_TO_SEND_PER_CALLBACK = 1000; //Max number of queued messages to send per periodic sending task

    // States of outgoing messages, incoming messages, and outgoing subscriptions
    private final Queue<Message> messagesToSend = new ConcurrentLinkedQueue<>();
    private String connectionId;
    private IotHubConnectionStatus state;
    private final String hostName;
    private final boolean isWebsocketConnection;
    private final DeviceClientConfig.AuthType authenticationType;
    private final Queue<DeviceClientConfig> deviceClientConfigs = new ConcurrentLinkedQueue<>();
    private IotHubListener listener;
    private TransportException savedException;
    private boolean reconnectionScheduled = false;
    private final Map<String, Boolean> reconnectionsScheduled = new ConcurrentHashMap<>();
    private ExecutorService executorService;

    // State latches are used for asynchronous open and close operations
    private CountDownLatch authenticationSessionOpenedLatch; // tracks if the authentication session has opened yet or not
    private Map<String, CountDownLatch> deviceSessionsOpenedLatches; // tracks if all expected device sessions have opened yet or not. Keys are deviceId's
    private CountDownLatch closeReactorLatch; // tracks if the reactor has been closed yet or not

    // Proton-j primitives and wrappers for the device and authentication sessions
    private Connection connection;
    private Reactor reactor;
    private Queue<AmqpsSessionHandler> sessionHandlers = new ConcurrentLinkedQueue<>();
    private Queue<AmqpsSasTokenRenewalHandler> sasTokenRenewalHandlers = new ConcurrentLinkedQueue<>();
    private AmqpsCbsSessionHandler amqpsCbsSessionHandler;

    // Multiplexed device registrations and un-registrations come from a non-reactor thread, so they get queued into these
    // queues and are executed when onTimerTask checks them.
    private final Queue<DeviceClientConfig> multiplexingClientsToRegister = new ConcurrentLinkedQueue<>();
    private final Queue<DeviceClientConfig> multiplexingClientsToUnregister = new ConcurrentLinkedQueue<>();

    public AmqpsIotHubConnection(DeviceClientConfig config)
    {
        this.deviceClientConfigs.add(config);

        this.isWebsocketConnection = config.isUseWebsocket();
        this.authenticationType = config.getAuthenticationType();

        String gatewayHostname = config.getGatewayHostname();
        if (gatewayHostname != null && !gatewayHostname.isEmpty())
        {
            log.debug("Gateway hostname was present in config, connecting to gateway rather than directly to hub");
            this.hostName = gatewayHostname;
        }
        else
        {
            log.trace("No gateway hostname was present in config, connecting directly to hub");
            this.hostName = config.getIotHubHostname();
        }

        add(new Handshaker());

        this.state = IotHubConnectionStatus.DISCONNECTED;
        log.trace("AmqpsIotHubConnection object is created successfully and will use port {}", this.isWebsocketConnection ? WEB_SOCKET_PORT : AMQP_PORT);
    }

    public void registerMultiplexedDevice(DeviceClientConfig config)
    {
        if (this.state == IotHubConnectionStatus.CONNECTED)
        {
            // session opening logic should be done from a proton reactor thread, not this thread. This queue gets polled
            // onTimerTask so that this client gets registered on that thread instead.
            log.trace("Queuing the registration of device {} to an active multiplexed connection", config.getDeviceId());
            deviceSessionsOpenedLatches.put(config.getDeviceId(), new CountDownLatch(1));
            this.multiplexingClientsToRegister.add(config);
        }

        if (!deviceClientConfigs.contains(config))
        {
            deviceClientConfigs.add(config);
        }
    }

    public void unregisterMultiplexedDevice(DeviceClientConfig config)
    {
        if (this.state == IotHubConnectionStatus.CONNECTED)
        {
            // session closing logic should be done from a proton reactor thread, not this thread. This queue gets polled
            // onTimerTask so that this client gets unregistered on that thread instead.
            log.trace("Queuing the unregistration of device {} from an active multiplexed connection", config.getDeviceId());
            this.multiplexingClientsToUnregister.add(config);
        }

        deviceClientConfigs.remove(config);
    }

    public void open() throws TransportException
    {
        log.debug("Opening amqp layer...");
        reconnectionScheduled = false;
        connectionId = UUID.randomUUID().toString();

        this.savedException = null;

        if (this.state == IotHubConnectionStatus.DISCONNECTED)
        {
            for (DeviceClientConfig clientConfig : deviceClientConfigs)
            {
                this.createSessionHandler(clientConfig);
            }

            initializeStateLatches();

            try
            {
                this.openAsync();

                log.trace("Waiting for authentication links to open...");
                boolean authenticationSessionOpenTimedOut = !this.authenticationSessionOpenedLatch.await(this.deviceClientConfigs.peek().getAmqpOpenAuthenticationSessionTimeout(), TimeUnit.SECONDS);

                if (this.savedException != null)
                {
                    throw this.savedException;
                }

                if (authenticationSessionOpenTimedOut)
                {
                    closeConnectionWithException("Timed out waiting for authentication session to open", true);
                }

                log.trace("Waiting for device sessions to open...");
                boolean deviceSessionsOpenTimedOut = false;
                for (DeviceClientConfig config : this.deviceClientConfigs)
                {
                    //Each device has its own worker session timeout according to its config settings
                    long workerSessionTimeoutTimeRemaining = config.getAmqpOpenDeviceSessionsTimeout();
                    deviceSessionsOpenTimedOut = !this.deviceSessionsOpenedLatches.get(config.getDeviceId()).await(workerSessionTimeoutTimeRemaining, TimeUnit.SECONDS);

                    if (deviceSessionsOpenTimedOut)
                    {
                        // If any device session times out while opening, don't wait for the others
                        break;
                    }
                }

                if (this.savedException != null)
                {
                    throw this.savedException;
                }

                if (deviceSessionsOpenTimedOut)
                {
                    closeConnectionWithException("Timed out waiting for worker links to open", true);
                }
            }
            catch (InterruptedException e)
            {
                executorServicesCleanup();
                throw new TransportException("Interrupted while waiting for links to open for AMQP connection", e);
            }
        }

        this.state = IotHubConnectionStatus.CONNECTED;
        this.listener.onConnectionEstablished(this.connectionId);

        log.debug("Amqp connection opened successfully");
    }

    public void close() throws TransportException
    {
        log.debug("Shutting down amqp layer...");
        closeAsync();

        try
        {
            log.trace("Waiting for reactor to close...");
            closeReactorLatch.await(MAX_WAIT_TO_CLOSE_CONNECTION, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e)
        {
            throw new TransportException("Interrupted while closing proton reactor", e);
        }

        this.executorServicesCleanup();

        log.trace("Amqp connection closed successfully");
        this.state = IotHubConnectionStatus.DISCONNECTED;
    }

    @Override
    public void onReactorInit(Event event)
    {
        this.reactor = event.getReactor();

        String hostName = this.hostName;
        int port = AMQP_PORT;

        if (this.isWebsocketConnection)
        {
            ProxySettings proxySettings = this.deviceClientConfigs.peek().getProxySettings();
            if (proxySettings != null)
            {
                hostName = proxySettings.getHostname();
                port = proxySettings.getPort();
            }
            else
            {
                port = WEB_SOCKET_PORT;
            }
        }

        this.reactor.connectionToHost(hostName, port, this);
        this.reactor.schedule(SEND_MESSAGES_PERIOD_MILLIS, this);
    }

    @Override
    public void onReactorFinal(Event event)
    {
        log.trace("Amqps reactor finalized");
        releaseLatch(authenticationSessionOpenedLatch);
        releaseDeviceSessionLatches();
        releaseLatch(closeReactorLatch);

        if (this.savedException != null)
        {
            this.scheduleReconnection(this.savedException);
        }
    }

    @Override
    public void onConnectionInit(Event event)
    {
        this.connection = event.getConnection();
        this.connection.setHostname(hostName);
        this.connection.open();
    }

    @Override
    public void onConnectionBound(Event event)
    {
        Transport transport = event.getTransport();

        if (this.isWebsocketConnection)
        {
            addWebSocketLayer(transport);
        }

        try
        {
            SSLContext sslContext = this.deviceClientConfigs.peek().getAuthenticationProvider().getSSLContext();
            if (this.authenticationType == DeviceClientConfig.AuthType.SAS_TOKEN)
            {
                Sasl sasl = transport.sasl();
                sasl.setMechanisms("ANONYMOUS");
            }

            SslDomain domain = Proton.sslDomain();
            domain.setSslContext(sslContext);
            domain.setPeerAuthentication(SslDomain.VerifyMode.VERIFY_PEER);
            domain.init(SslDomain.Mode.CLIENT);
            transport.ssl(domain);
        }
        catch (IOException e)
        {
            this.savedException = new TransportException(e);
            log.error("Encountered an exception while setting ssl domain for the amqp connection", this.savedException);
        }

        // Adding proxy layer needs to be done after sending SSL message
        if (this.deviceClientConfigs.peek().getProxySettings() != null)
        {
            addProxyLayer(transport, event.getConnection().getHostname() + ":" + WEB_SOCKET_PORT);
        }
    }

    @Override
    public void onConnectionLocalOpen(Event event)
    {
        log.trace("Amqp connection opened locally");

        //Create one session per multiplexed device, or just one session if not multiplexing
        if (this.authenticationType == DeviceClientConfig.AuthType.SAS_TOKEN)
        {
            // The CBS ("Claims-Based-Security") session is dedicated to sending SAS tokens to the service to authenticate
            // all of the device sessions in this AMQP connection.
            Session cbsSession = connection.session();

            amqpsCbsSessionHandler = new AmqpsCbsSessionHandler(cbsSession, this);

            // sas token handler list has no information that needs to be carried over after a reconnect, so close and
            // clear the list and add a new handler to the list for each device session.
            for (AmqpsSasTokenRenewalHandler sasTokenRenewalHandler : this.sasTokenRenewalHandlers)
            {
                sasTokenRenewalHandler.close();
            }
            sasTokenRenewalHandlers.clear();

            // Open a device session per device, and create a sas token renewal handler for each device session
            for (AmqpsSessionHandler amqpsSessionHandler : this.sessionHandlers)
            {
                amqpsSessionHandler.setSession(connection.session());
                sasTokenRenewalHandlers.add(new AmqpsSasTokenRenewalHandler(amqpsCbsSessionHandler, amqpsSessionHandler));
            }
        }
        else
        {
            // should only be one session since x509 doesn't support multiplexing, so just get the first in the list
            AmqpsSessionHandler amqpsSessionHandler = this.sessionHandlers.peek();
            amqpsSessionHandler.setSession(connection.session());
        }
    }

    @Override
    public void onConnectionRemoteOpen(Event event)
    {
        log.trace("Amqp connection opened remotely");
    }

    @Override
    public void onConnectionLocalClose(Event event)
    {
        log.debug("Amqp connection closed locally, shutting down all active sessions...");
        for (AmqpsSessionHandler amqpSessionHandler : sessionHandlers)
        {
            amqpSessionHandler.closeSession();
        }

        // cbs session handler is only null if using x509 auth
        if (amqpsCbsSessionHandler != null)
        {
            log.debug("Shutting down cbs session...");
            amqpsCbsSessionHandler.close();
        }

        log.trace("Closing reactor since connection has closed");
        event.getReactor().stop();
    }

    @Override
    public void onConnectionRemoteClose(Event event)
    {
        Connection connection = event.getConnection();
        if (connection.getLocalState() == EndpointState.ACTIVE)
        {
            ErrorCondition errorCondition = connection.getRemoteCondition();
            this.savedException = AmqpsExceptionTranslator.convertFromAmqpException(errorCondition);
            log.error("Amqp connection was closed remotely", this.savedException);
            this.connection.close();
        }
        else
        {
            log.trace("Closing reactor since connection has closed");
            event.getReactor().stop();
        }
    }

    @Override
    public void onTransportError(Event event)
    {
        super.onTransportError(event);
        this.state = IotHubConnectionStatus.DISCONNECTED;

        //Error may be on remote, and it may be local
        ErrorCondition errorCondition = event.getTransport().getRemoteCondition();

        //Sometimes the remote errorCondition object is not null, but all of its fields are null. In this case, check the local error condition
        //for the error details.
        if (errorCondition == null || (errorCondition.getCondition() == null && errorCondition.getDescription() == null && errorCondition.getInfo() == null))
        {
            errorCondition = event.getTransport().getCondition();
        }

        this.savedException = AmqpsExceptionTranslator.convertFromAmqpException(errorCondition);

        log.error("Amqp transport error occurred, closing the amqps connection", this.savedException);
        event.getConnection().close();
    }

    @Override
    public void onTimerTask(Event event)
    {
        sendQueuedMessages();

        checkForNewlyUnregisteredMultiplexedClientsToStop();
        checkForNewlyRegisteredMultiplexedClientsToStart();

        event.getReactor().schedule(SEND_MESSAGES_PERIOD_MILLIS, this);
    }

    @Override
    public void setListener(IotHubListener listener)
    {
        this.listener = listener;
    }

    @Override
    public IotHubStatusCode sendMessage(com.microsoft.azure.sdk.iot.device.Message message)
    {
        // Note that you cannot just send this message from this thread. Proton-j's reactor is not thread safe. As such,
        // all message sending must be done from the proton-j thread that is exposed to this SDK through callbacks
        // such as onLinkFlow(), or onTimerTask()
        log.trace("Adding message to amqp message queue to be sent later ({})", message);
        messagesToSend.add(message);
        return IotHubStatusCode.OK;
    }

    @Override
    public boolean sendMessageResult(IotHubTransportMessage message, IotHubMessageResult result)
    {
        DeliveryState ackType;

        // Complete/Abandon/Reject is an IoTHub concept. For AMQP, they map to Accepted/Released/Rejected.
        if (result == IotHubMessageResult.ABANDON)
        {
            ackType = Released.getInstance();
        }
        else if (result == IotHubMessageResult.REJECT)
        {
            ackType = new Rejected();
        }
        else if (result == IotHubMessageResult.COMPLETE)
        {
            ackType = Accepted.getInstance();
        }
        else
        {
            log.warn("Invalid IoT Hub message result {}", result.name());
            return false;
        }

        //Check each session handler to see who is responsible for sending this acknowledgement
        for (AmqpsSessionHandler sessionHandler : sessionHandlers)
        {
            if (sessionHandler.acknowledgeReceivedMessage(message, ackType))
            {
                return true;
            }
        }

        log.warn("No sessions could acknowledge the message ({})", message);
        return false;
    }

    @Override
    public String getConnectionId()
    {
        return this.connectionId;
    }

    @Override
    public void onDeviceSessionOpened(String deviceId)
    {
        if (this.deviceSessionsOpenedLatches.containsKey(deviceId))
        {
            log.trace("Device session for device {} opened, counting down the device sessions opening latch");
            this.deviceSessionsOpenedLatches.get(deviceId).countDown();
            this.listener.onMultiplexedDeviceSessionEstablished(this.connectionId, deviceId);
        }
        else
        {
            log.warn("Unrecognized deviceId {} reported its device session as opened, ignoring it.", deviceId);
        }
    }

    @Override
    public void onAuthenticationSessionOpened()
    {
        log.trace("Authentication session opened, counting down the authentication session opening latch");
        this.authenticationSessionOpenedLatch.countDown();

        if (this.authenticationType == DeviceClientConfig.AuthType.SAS_TOKEN)
        {
            // AMQPS_WS has an issue where the remote host kills the connection if 35+ multiplexed devices are all
            // authenticated at once. To work around this, the authentications will be done in serial instead of in parallel.
            // Once a given device's authentication finishes, it will trigger the next authentication
            AmqpsSasTokenRenewalHandler previousHandler = null;
            for (AmqpsSasTokenRenewalHandler amqpsSasTokenRenewalHandler : sasTokenRenewalHandlers)
            {
                if (previousHandler != null)
                {
                    previousHandler.setNextToAuthenticate(amqpsSasTokenRenewalHandler);
                }

                previousHandler = amqpsSasTokenRenewalHandler;
            }

            try
            {
                // Sending the first authentication message will eventually trigger sending the second, which will trigger the third, and so on.
                sasTokenRenewalHandlers.peek().sendAuthenticationMessage(this.connection.getReactor());
            }
            catch (TransportException e)
            {
                log.error("Failed to send CBS authentication message", e);
                this.savedException = e;
            }

        }
    }

    @Override
    public void onMessageAcknowledged(Message message, DeliveryState deliveryState)
    {
        if (deliveryState == Accepted.getInstance())
        {
            this.listener.onMessageSent(message, null);
        }
        else if (deliveryState instanceof Rejected)
        {
            // The message was not accepted by the server, and the reason why is found within the nested error
            TransportException ex = AmqpsExceptionTranslator.convertFromAmqpException(((Rejected) deliveryState).getError());
            this.listener.onMessageSent(message, ex);
        }
        else if (deliveryState == Released.getInstance())
        {
            // As per AMQP spec, this state means the message should be re-delivered to the server at a later time
            ProtocolException protocolException = new ProtocolException("Message was released by the amqp server");
            protocolException.setRetryable(true);
            this.listener.onMessageSent(message, protocolException);
        }
        else
        {
            log.warn("Unexpected delivery state for sent message ({})", message);
        }
    }

    @Override
    public void onMessageReceived(IotHubTransportMessage message)
    {
        this.listener.onMessageReceived(message, null);
    }

    @Override
    public void onAuthenticationFailed(TransportException transportException)
    {
        this.savedException = transportException;
        releaseLatch(authenticationSessionOpenedLatch);
        releaseDeviceSessionLatches();
    }

    @Override
    public void onSessionClosedUnexpectedly(ErrorCondition errorCondition, String deviceId)
    {
        TransportException savedException = AmqpsExceptionTranslator.convertFromAmqpException(errorCondition);

        if (this.deviceClientConfigs.size() > 1)
        {
            // When multiplexing, don't kill the connection just because a session dropped.
            log.error("Amqp session closed unexpectedly. notifying the transport layer to start reconnection logic...", this.savedException);
            scheduleDeviceSessionReconnection(savedException, deviceId);
        }
        else
        {
            // When not multiplexing, reconnection logic will just spin up the whole connection again.
            this.savedException = savedException;
            log.error("Amqp session closed unexpectedly. Closing this connection...", this.savedException);
            this.connection.close();
        }
    }

    @Override
    public void onCBSSessionClosedUnexpectedly(ErrorCondition errorCondition)
    {
        this.savedException = AmqpsExceptionTranslator.convertFromAmqpException(errorCondition);
        log.error("Amqp CBS session closed unexpectedly. Closing this connection...", this.savedException);
        this.connection.close();
    }

    @Override
    public void onSessionClosedAsExpected(String deviceId)
    {
        // don't want to signal Client_Close to transport layer if this is in the middle of a disconnected_retrying event
        if (this.reconnectionsScheduled.get(deviceId) == null || this.reconnectionsScheduled.get(deviceId) == false)
        {
            log.trace("onSessionClosedAsExpected callback executed, notifying transport layer");
            this.listener.onMultiplexedDeviceSessionLost(null, this.connectionId, deviceId);
        }
    }

    private void addWebSocketLayer(Transport transport)
    {
        log.debug("Adding websocket layer to amqp transport");
        WebSocketImpl webSocket = new WebSocketImpl(MAX_MESSAGE_PAYLOAD_SIZE);
        webSocket.configure(this.hostName, WEB_SOCKET_PATH, WEB_SOCKET_QUERY, WEB_SOCKET_PORT, WEB_SOCKET_SUB_PROTOCOL, null, null);
        ((TransportInternal) transport).addTransportLayer(webSocket);
    }

    private void addProxyLayer(Transport transport, String hostName)
    {
        log.debug("Adding proxy layer to amqp transport");
        ProxySettings proxySettings = this.deviceClientConfigs.peek().getProxySettings();

        ProxyImpl proxy;

        if (proxySettings.getUsername() != null && proxySettings.getPassword() != null)
        {
            log.trace("Adding proxy username and password to amqp proxy configuration");
            ProxyConfiguration proxyConfiguration = new ProxyConfiguration(ProxyAuthenticationType.BASIC, proxySettings.getProxy(), proxySettings.getUsername(), new String(proxySettings.getPassword()));
            proxy = new ProxyImpl(proxyConfiguration);
        }
        else
        {
            log.trace("No proxy username and password will be used amqp proxy configuration");
            proxy = new ProxyImpl();
        }

        final ProxyHandler proxyHandler = new ProxyHandlerImpl();
        proxy.configure(hostName, null, proxyHandler, transport);
        ((TransportInternal) transport).addTransportLayer(proxy);
    }

    private void sendQueuedMessages()
    {
        int messagesAttemptedToBeProcessed = 0;
        Message message = messagesToSend.poll();
        while (message != null && messagesAttemptedToBeProcessed < MAX_MESSAGES_TO_SEND_PER_CALLBACK)
        {
            messagesAttemptedToBeProcessed++;
            boolean lastSendSucceeded = sendQueuedMessage(message);

            if (!lastSendSucceeded)
            {
                //message failed to send, likely due to lack of link credit available. Re-queue and try again later
                log.trace("Amqp message failed to send, adding it back to messages to send queue ({})", message);
                messagesToSend.add(message);
                return;
            }

            message = messagesToSend.poll();
        }

        if (message != null)
        {
            //message was polled out of list, but loop exited from processing too many messages before it could process this message, so re-queue it for later
            messagesToSend.add(message);
        }
    }

    private boolean sendQueuedMessage(Message message)
    {
        boolean sendSucceeded = false;

        log.trace("Sending message over amqp ({})", message);

        for (AmqpsSessionHandler sessionHandler : this.sessionHandlers)
        {
            sendSucceeded = sessionHandler.sendMessage(message);

            if (sendSucceeded)
            {
                break;
            }
        }

        return sendSucceeded;
    }

    private Reactor createReactor() throws TransportException
    {
        try
        {
            if (this.authenticationType == DeviceClientConfig.AuthType.X509_CERTIFICATE)
            {
                ReactorOptions options = new ReactorOptions();
                options.setEnableSaslByDefault(false);
                return Proton.reactor(options, this);
            }
            else
            {
                return Proton.reactor(this);
            }
        }
        catch (IOException e)
        {
            throw new TransportException("Could not create Proton reactor", e);
        }
    }

    private void scheduleReconnection(Throwable throwable)
    {
        if (!reconnectionScheduled)
        {
            reconnectionScheduled = true;
            log.warn("Amqp connection was closed, creating a thread to notify transport layer", throwable);
            ReconnectionNotifier.notifyDisconnectAsync(throwable, this.listener, this.connectionId);
        }
    }

    private void scheduleDeviceSessionReconnection(Throwable throwable, String deviceId)
    {
        if (this.reconnectionsScheduled.get(deviceId) == null || this.reconnectionsScheduled.get(deviceId) == false)
        {
            this.reconnectionsScheduled.put(deviceId, true);
            log.warn("Amqp session for device {} was closed, creating a thread to notify transport layer", deviceId, throwable);
            ReconnectionNotifier.notifyDeviceDisconnectAsync(throwable, this.listener, this.connectionId, deviceId);
        }
    }

    private void releaseLatch(CountDownLatch latch)
    {
        for (int i = 0; i < latch.getCount(); i++)
        {
            latch.countDown();
        }
    }

    private void releaseDeviceSessionLatches()
    {
        for (String deviceId : this.deviceSessionsOpenedLatches.keySet())
        {
            releaseLatch(this.deviceSessionsOpenedLatches.get(deviceId));
        }
    }

    private AmqpsSessionHandler createSessionHandler(DeviceClientConfig deviceClientConfig)
    {
        // Check if the device session still exists from a previous connection
        AmqpsSessionHandler amqpsSessionHandler = null;
        for (AmqpsSessionHandler existingAmqpsSessionHandler : this.sessionHandlers)
        {
            if (existingAmqpsSessionHandler.getDeviceId().equals(deviceClientConfig.getDeviceId()))
            {
                amqpsSessionHandler = existingAmqpsSessionHandler;
                break;
            }
        }

        // If the device session did not exist in the previous connection, or if there was no previous connection,
        // create a new session
        if (amqpsSessionHandler == null)
        {
            amqpsSessionHandler = new AmqpsSessionHandler(deviceClientConfig, this);
            this.sessionHandlers.add(amqpsSessionHandler);
        }

        return amqpsSessionHandler;
    }

    // This function is called periodically from the onTimerTask reactor callback so that any newly registered device sessions
    // can be opened on a reactor thread instead of from one of our threads.
    private void checkForNewlyRegisteredMultiplexedClientsToStart()
    {
        DeviceClientConfig configToRegister = this.multiplexingClientsToRegister.poll();
        while (configToRegister != null)
        {
            AmqpsSessionHandler amqpsSessionHandler = createSessionHandler(configToRegister);

            log.trace("Adding device session for device {} to an active connection", configToRegister.getDeviceId());
            amqpsSessionHandler.setSession(this.connection.session());
            AmqpsSasTokenRenewalHandler amqpsSasTokenRenewalHandler = new AmqpsSasTokenRenewalHandler(amqpsCbsSessionHandler, amqpsSessionHandler);
            sasTokenRenewalHandlers.add(amqpsSasTokenRenewalHandler);
            try
            {
                amqpsSasTokenRenewalHandler.sendAuthenticationMessage(this.reactor);
            }
            catch (TransportException e)
            {
                log.warn("Failed to send authentication message for device {}; will try again.", amqpsSasTokenRenewalHandler.amqpsSessionHandler.getDeviceId());

                //sas token renewal handler will be recreated when this function gets called again.
                amqpsSasTokenRenewalHandler.close();
                sasTokenRenewalHandlers.remove(amqpsSasTokenRenewalHandler);

                //requeue the device that should re-attempt registration so that this function gets called again later
                this.multiplexingClientsToRegister.add(configToRegister);
                return;
            }

            configToRegister = this.multiplexingClientsToRegister.poll();
        }
    }

    // This function is called periodically from the onTimerTask reactor callback so that any newly registered device sessions
    // can be opened on a reactor thread instead of from one of our threads.
    private void checkForNewlyUnregisteredMultiplexedClientsToStop()
    {
        DeviceClientConfig configToUnregister = this.multiplexingClientsToUnregister.poll();
        while (configToUnregister != null)
        {
            // Check if the device session still exists from a previous connection
            AmqpsSessionHandler amqpsSessionHandler = null;
            for (AmqpsSessionHandler existingAmqpsSessionHandler : this.sessionHandlers)
            {
                if (existingAmqpsSessionHandler.getDeviceId().equals(configToUnregister.getDeviceId()))
                {
                    amqpsSessionHandler = existingAmqpsSessionHandler;
                    break;
                }
            }

            // If a device session currently exists for this device identity, then remove it
            if (amqpsSessionHandler != null)
            {
                log.trace("Removing session handler for device {}", amqpsSessionHandler.getDeviceId());
                this.sessionHandlers.remove(amqpsSessionHandler);
            }
            else
            {
                log.warn("Attempted to remove device session for device {} from multiplexed connection, but device was not currently registered.", configToUnregister.getDeviceId());
                return;
            }

            // Need to find the sas token renewal handler that is tied to this device
            AmqpsSasTokenRenewalHandler sasTokenRenewalHandlerToRemove = null;
            for (AmqpsSasTokenRenewalHandler existingSasTokenRenewalHandler : this.sasTokenRenewalHandlers)
            {
                if (existingSasTokenRenewalHandler.amqpsSessionHandler.getDeviceId().equals(configToUnregister.getDeviceId()))
                {
                    sasTokenRenewalHandlerToRemove = existingSasTokenRenewalHandler;

                    // Stop the sas token renewal handler from sending any more authentication messages on behalf of this device
                    log.trace("Closing sas token renewal handler for device {}", configToUnregister.getDeviceId());
                    sasTokenRenewalHandlerToRemove.close();
                    break;
                }
            }

            if (sasTokenRenewalHandlerToRemove != null)
            {
                this.sasTokenRenewalHandlers.remove(sasTokenRenewalHandlerToRemove);
            }

            this.reconnectionsScheduled.remove(configToUnregister.getDeviceId());

            log.debug("Closing device session for multiplexed device {}", configToUnregister.getDeviceId());
            amqpsSessionHandler.closeSession();

            configToUnregister = this.multiplexingClientsToUnregister.poll();
        }
    }

    private void initializeStateLatches()
    {
        this.closeReactorLatch = new CountDownLatch(REACTOR_COUNT);

        if (this.authenticationType == DeviceClientConfig.AuthType.SAS_TOKEN)
        {
            log.trace("Initializing authentication link latch count to {}", CBS_SESSION_COUNT);
            this.authenticationSessionOpenedLatch = new CountDownLatch(CBS_SESSION_COUNT);
        }
        else
        {
            log.trace("Initializing authentication link latch count to 0 because x509 connections don't have authentication links");
            this.authenticationSessionOpenedLatch = new CountDownLatch(0);
        }

        this.deviceSessionsOpenedLatches = new ConcurrentHashMap<>();
        for (AmqpsSessionHandler sessionHandler : sessionHandlers)
        {
            String deviceId = sessionHandler.getDeviceId();
            log.trace("Initializing device session latch for device {}", deviceId);
            this.deviceSessionsOpenedLatches.put(deviceId, new CountDownLatch(1));
        }
    }

    private void closeConnectionWithException(String errorMessage, boolean isRetryable) throws TransportException
    {
        TransportException transportException = new TransportException(errorMessage);
        transportException.setRetryable(isRetryable);
        log.error(errorMessage, transportException);

        this.close();
        throw transportException;
    }

    private void openAsync() throws TransportException
    {
        log.trace("OpenAsnyc called for amqp connection");

        if (executorService == null)
        {
            log.trace("Creating new executor service");
            executorService = Executors.newFixedThreadPool(1);
        }

        this.reactor = createReactor();
        ReactorRunner reactorRunner = new ReactorRunner(new IotHubReactor(this.reactor), this.listener, this.connectionId);
        executorService.submit(reactorRunner);
    }

    private void closeAsync()
    {
        log.trace("OpenAsync called for amqp connection");

        // This may be called before a connection or reactor have been established, so need to check the state
        if (this.connection == null && this.reactor == null) {
            // If both the connection and reactor were never initialized, then just release the latches to signal the end of the connection closing
            releaseLatch(authenticationSessionOpenedLatch);
            releaseDeviceSessionLatches();
            releaseLatch(closeReactorLatch);
        }
        else if (this.connection == null)
        {
            // If only the reactor was initialized, then just stop the reactor. OnReactorFinal will release the latches to signal the end of the connection closing
            this.reactor.stop();
        }
        else if (this.reactor == null)
        {
            // This should never happen as this block is only hit when the connection was initialized but its reactor was not
            log.warn("Connection was initialized without a reactor, connection is in an unknown state; closing connection anyways.");
            this.connection.close();
        }
        else if (this.connection.getLocalState() == EndpointState.CLOSED && this.connection.getRemoteState() == EndpointState.CLOSED)
        {
            log.trace("Closing amqp reactor since the connection was already closed");
            this.connection.getReactor().stop();
        }
        else
        {
            //client is initializing this close, so don't shut down the reactor yet
            log.trace("Closing amqp connection");
            this.connection.close();
        }
    }

    private void executorServicesCleanup()
    {
        if (this.executorService != null)
        {
            log.trace("Shutdown of executor service has started");
            this.executorService.shutdownNow();
            this.executorService = null;
            log.trace("Shutdown of executor service completed");
        }
    }

    private class ReactorRunner implements Callable
    {
        private static final String THREAD_NAME = "azure-iot-sdk-ReactorRunner";
        private final IotHubReactor iotHubReactor;
        private final IotHubListener listener;
        private String connectionId;

        ReactorRunner(IotHubReactor iotHubReactor, IotHubListener listener, String connectionId)
        {
            this.listener = listener;
            this.iotHubReactor = iotHubReactor;
            this.connectionId = connectionId;
        }

        @Override
        public Object call()
        {
            try
            {
                Thread.currentThread().setName(THREAD_NAME);
                iotHubReactor.run();
            }
            catch (HandlerException e)
            {
                TransportException transportException = new TransportException(e);

                // unclassified exceptions are treated as retryable in ProtonJExceptionParser, so they should be treated
                // the same way here. Exceptions caught here tend to be transient issues.
                transportException.setRetryable(true);

                this.listener.onConnectionLost(transportException, connectionId);
            }

            return null;
        }
    }
}
