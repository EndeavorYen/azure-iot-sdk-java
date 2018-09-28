/**
 * Code generated by Microsoft (R) AutoRest Code Generator.
 * Changes may cause incorrect behavior and will be lost if the code is
 * regenerated.
 */

package com.microsoft.azure.sdk.iot.provisioning.service.models;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Attestation via X509.
 */
public class X509Attestation {
    /**
     * The clientCertificates property.
     */
    @JsonProperty(value = "clientCertificates")
    private X509Certificates clientCertificates;

    /**
     * The signingCertificates property.
     */
    @JsonProperty(value = "signingCertificates")
    private X509Certificates signingCertificates;

    /**
     * The caReferences property.
     */
    @JsonProperty(value = "caReferences")
    private X509CAReferences caReferences;

    /**
     * Get the clientCertificates value.
     *
     * @return the clientCertificates value
     */
    public X509Certificates clientCertificates() {
        return this.clientCertificates;
    }

    /**
     * Set the clientCertificates value.
     *
     * @param clientCertificates the clientCertificates value to set
     * @return the X509Attestation object itself.
     */
    public X509Attestation withClientCertificates(X509Certificates clientCertificates) {
        this.clientCertificates = clientCertificates;
        return this;
    }

    /**
     * Get the signingCertificates value.
     *
     * @return the signingCertificates value
     */
    public X509Certificates signingCertificates() {
        return this.signingCertificates;
    }

    /**
     * Set the signingCertificates value.
     *
     * @param signingCertificates the signingCertificates value to set
     * @return the X509Attestation object itself.
     */
    public X509Attestation withSigningCertificates(X509Certificates signingCertificates) {
        this.signingCertificates = signingCertificates;
        return this;
    }

    /**
     * Get the caReferences value.
     *
     * @return the caReferences value
     */
    public X509CAReferences caReferences() {
        return this.caReferences;
    }

    /**
     * Set the caReferences value.
     *
     * @param caReferences the caReferences value to set
     * @return the X509Attestation object itself.
     */
    public X509Attestation withCaReferences(X509CAReferences caReferences) {
        this.caReferences = caReferences;
        return this;
    }

}
