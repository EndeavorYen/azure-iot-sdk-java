// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package tests.integration.com.microsoft.azure.sdk.iot.helpers;

import com.microsoft.azure.sdk.iot.device.IotHubConnectionString;
import com.microsoft.azure.sdk.iot.device.SasTokenProvider;
import com.microsoft.azure.sdk.iot.device.auth.IotHubSasToken;

import java.net.URISyntaxException;

/**
 * Basic implementation of the SasTokenProvider interface, for test purposes.
 */
public class SasTokenProviderImpl implements SasTokenProvider
{
    IotHubConnectionString deviceConnectionString;
    int expiryLengthSeconds;

    //TODO need to test proactive renewal for AMQP and reactive renewal for MQTT
    private static final int DEFAULT_EXPIRY_LENGTH_MILLISECONDS =  60 * 60 * 1000; // 1 hour

    public SasTokenProviderImpl(String deviceConnectionString) throws URISyntaxException {
        this(deviceConnectionString, DEFAULT_EXPIRY_LENGTH_MILLISECONDS);
    }

    public SasTokenProviderImpl(String deviceConnectionString, int expiryLengthSeconds) throws URISyntaxException {
        this.deviceConnectionString = new IotHubConnectionString(deviceConnectionString);
        this.expiryLengthSeconds = expiryLengthSeconds;
    }

    @Override
    public String getSasToken() {
        return new IotHubSasToken(
                deviceConnectionString.getHostName(),
                deviceConnectionString.getDeviceId(),
                deviceConnectionString.getSharedAccessKey(),
                null,
                deviceConnectionString.getModuleId(),
                expiryLengthSeconds + System.currentTimeMillis()).toString();
    }
}
