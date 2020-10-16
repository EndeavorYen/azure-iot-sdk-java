/*
*  Copyright (c) Microsoft. All rights reserved.
*  Licensed under the MIT license. See LICENSE file in the project root for full license information.
*/

package com.microsoft.azure.sdk.iot.device.auth;

import com.microsoft.azure.sdk.iot.device.SasTokenProvider;

import javax.net.ssl.SSLContext;

public class IotHubSasTokenProvidedAuthenticationProvider extends IotHubSasTokenAuthenticationProvider
{
    SasTokenProvider sasTokenProvider;
    String lastSasToken;
    long lastSasTokenAcquiryTimeMillis;

    public IotHubSasTokenProvidedAuthenticationProvider(String hostName, String deviceId, String moduleId, SasTokenProvider sasTokenProvider, SSLContext sslContext) {
        super(hostName, null, deviceId, moduleId, sslContext);

        if (sasTokenProvider == null)
        {
            throw new IllegalArgumentException("sas token provider cannot be null");
        }

        this.sasTokenProvider = sasTokenProvider;
    }

    @Override
    public boolean isRenewalNecessary()
    {
        return false;
    }

    @Override
    public void setTokenValidSecs(long tokenValidSecs)
    {
        throw new UnsupportedOperationException("Cannot configure sas token time to live when custom sas token provider is in use");
    }

    @Override
    public boolean canRefreshToken()
    {
        return true;
    }

    @Override
    public String getRenewedSasToken(boolean forceRenewal)
    {
        if (forceRenewal || lastSasToken == null)
        {
            lastSasToken = sasTokenProvider.getSasToken();
            lastSasTokenAcquiryTimeMillis = System.currentTimeMillis();
        }

        return lastSasToken;
    }

    @Override
    public int getMillisecondsBeforeProactiveRenewal()
    {
        //TODO need to validate this a bit more
        long expiryTime = IotHubSasToken.getExpiryTimeFromToken(lastSasToken);

        // Assuming that when we got the sas token is when it's life "starts" for the sake of figuring out when
        long tokenValidMilliseconds = expiryTime - lastSasTokenAcquiryTimeMillis;

        double timeBufferMultiplier = this.timeBufferPercentage / 100.0; //Convert 85 to .85, for example. Percentage multipliers are in decimal
        return (int) (tokenValidMilliseconds * timeBufferMultiplier);
    }
}
