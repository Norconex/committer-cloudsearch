/* Copyright 2016-2017 Norconex Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.norconex.committer.cloudsearch;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomain;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomainClientBuilder;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsRequest;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsResult;
import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.StringUtil;
import com.norconex.commons.lang.encrypt.EncryptionKey;
import com.norconex.commons.lang.encrypt.EncryptionUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.time.DurationParser;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;

/**
 * <p>
 * Commits documents to Amazon CloudSearch.
 * </p>
 * <h3>Authentication:</h3>
 * <p>
 * An access key and security key are required to connect to interact with 
 * CloudSearch. For enhanced security, it is best to use one of the methods
 * described in {@link DefaultAWSCredentialsProviderChain} for setting them
 * (environment variables, system properties, profile file, etc). 
 * Do not explicitly set "accessKey" and "secretKey" on this class if you 
 * want to rely on safer methods.
 * </p>
 * <h3>CloudSearch ID limitations:</h3>
 * <p>
 * As of this writing, CloudSearch has a 128 characters limitation 
 * on its "id" field. In addition, certain characters are not allowed.
 * By default, an error will result from trying to submit
 * documents with an invalid ID. <b>As of 1.3.0</b>, you can get around this by
 * setting {@link #setFixBadIds(boolean)} to <code>true</code>.  It will
 * truncate references that are too long and append a hash code to it
 * representing the truncated part.  It will also convert invalid
 * characters to underscore.  This approach is not 100% 
 * collision-free (uniqueness), but it should safely cover the vast 
 * majority of cases. 
 * </p>
 * <p>
 * If you want to keep the original (non-truncated) URL, make sure you set 
 * {@link #setKeepSourceReferenceField(boolean)} to <code>true</code>.
 * </p>
 *
 * <h3>Password encryption in XML configuration:</h3>
 * <p>
 * As of 1.4.0, it is possible to specify proxy settings, and optionally, have
 * the supplied password encrypted using {@link EncryptionUtil} or 
 * encrypt/decrypt scripts package with this library. 
 * In order for the password to be decrypted properly by the crawler, you need
 * to specify the encryption key used to encrypt it. The key can be stored
 * in a few supported locations and a combination of 
 * <code>proxyPasswordKey</code>
 * and <code>proxyPasswordKeySource</code> must be specified to properly
 * locate the key. The supported sources are:
 * </p> 
 * <table border="1" summary="">
 *   <tr>
 *     <th><code>proxyPasswordKeySource</code></th>
 *     <th><code>proxyPasswordKey</code></th>
 *   </tr>
 *   <tr>
 *     <td><code>key</code></td>
 *     <td>The actual encryption key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>file</code></td>
 *     <td>Path to a file containing the encryption key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>environment</code></td>
 *     <td>Name of an environment variable containing the key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>property</code></td>
 *     <td>Name of a JVM system property containing the key.</td>
 *   </tr>
 * </table>
 * 
 * <h3>XML configuration usage:</h3>
 * 
 * <pre>
 *  &lt;committer class="com.norconex.committer.cloudsearch.CloudSearchCommitter"&gt;
 *  
 *      &lt;!-- Mandatory: --&gt;
 *      &lt;serviceEndpoint&gt;(CloudSearch service endpoint)&lt;/serviceEndpoint&gt;
 *      
 *      &lt;!-- Mandatory if not configured elsewhere: --&gt;
 *      &lt;accessKey&gt;
 *         (Optional CloudSearch access key. Will be taken from environment 
 *          when blank.)
 *      &lt;/accessKey&gt;
 *      &lt;secretKey&gt;
 *         (Optional CloudSearch secret key. Will be taken from environment
 *          when blank.)
 *      &lt;/secretKey&gt;
 *      
 *      &lt;!-- Optional settings: --&gt;
 *      &lt;fixBadIds&gt;
 *         [false|true](Forces references to fit into a CloudSearch id field.)
 *      &lt;/fixBadIds&gt;
 *      &lt;signingRegion&gt;(CloudSearch signing region)&lt;/signingRegion&gt;
 *      &lt;proxyHost&gt;...&lt;/proxyHost&gt;
 *      &lt;proxyPort&gt;...&lt;/proxyPort&gt;
 *      &lt;proxyUsername&gt;...&lt;/proxyUsername&gt;
 *      &lt;proxyPassword&gt;...&lt;/proxyPassword&gt;
 *      &lt;!-- Use the following if password is encrypted. --&gt;
 *      &lt;proxyPasswordKey&gt;(the encryption key or a reference to it)&lt;/proxyPasswordKey&gt;
 *      &lt;proxyPasswordKeySource&gt;[key|file|environment|property]&lt;/proxyPasswordKeySource&gt;
 *      
 *      &lt;sourceReferenceField keep="[false|true]"&gt;
 *         (Optional name of field that contains the document reference, when 
 *         the default document reference is not used.  The reference value
 *         will be mapped to CloudSearch "id" field, which is mandatory.
 *         Once re-mapped, this metadata source field is 
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceReferenceField&gt;
 *      &lt;sourceContentField keep="[false|true]"&gt;
 *         (If you wish to use a metadata field to act as the document 
 *         "content", you can specify that field here.  Default 
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceContentField&gt;
 *      &lt;targetContentField&gt;
 *         (CloudSearch target field name for a document content/body.
 *          Default is: content)
 *      &lt;/targetContentField&gt;
 *      &lt;commitBatchSize&gt;
 *          (Max number of docs to send CloudSearch at once. If you experience
 *           memory problems, lower this number.  Default is 100.)
 *      &lt;/commitBatchSize&gt;
 *      &lt;queueDir&gt;(Optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;
 *          (Max queue size before committing. Default is 1000.)
 *      &lt;/queueSize&gt;
 *      &lt;maxRetries&gt;
 *          (Max retries upon commit failures. Default is 0.)
 *      &lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;
 *          (Max delay in milliseconds between retries. Default is 0.)
 *      &lt;/maxRetryWait&gt;
 *  &lt;/committer&gt;
 * </pre>
 * 
 * <p>
 * XML configuration entries expecting millisecond durations
 * can be provided in human-readable format (English only), as per 
 * {@link DurationParser} (e.g., "5 minutes and 30 seconds" or "5m30s").
 * </p> 
 * 
 * @author El-Hebri Khiari
 * @author Pascal Essiembre
 */
public class CloudSearchCommitter extends AbstractMappedCommitter {

    private static final Logger LOG = 
            LogManager.getLogger(CloudSearchCommitter.class);
    
    /** 
     * CouldSearch mandatory field pattern. Characters not matching 
     * the pattern will be replaced by an underscore.
     */
    public static final Pattern FIELD_PATTERN = Pattern.compile(
            "[a-z0-9][a-z0-9_]{0,63}$");
    
    /** CloudSearch mandatory ID field */
    public static final String COULDSEARCH_ID_FIELD = "id";
    /** Default CloudSearch content field */
    public static final String DEFAULT_COULDSEARCH_CONTENT_FIELD = "content";
    
    private static final String TEMP_TARGET_ID_FIELD = "__nx.cloudsearch.id";
    
    private AmazonCloudSearchDomain awsClient;
    private boolean needNewAwsClient = true;
    
    private String serviceEndpoint;
    private String signingRegion;
    private String accessKey;
    private String secretKey;
    private boolean fixBadIds;
    private String proxyHost = null;
    private int proxyPort = 8080;
    private String proxyUsername = null;
    private String proxyPassword = null;
    private EncryptionKey proxyPasswordKey;    
    
    public CloudSearchCommitter() {
        this(null);
    }
    public CloudSearchCommitter(String serviceEndpoint) {
        this(serviceEndpoint, null);
    }
    
    public CloudSearchCommitter(String serviceEndpoint, String signingRegion) {
        super();
        this.serviceEndpoint = serviceEndpoint;
        this.signingRegion = signingRegion;
        setTargetContentField(DEFAULT_COULDSEARCH_CONTENT_FIELD);
        super.setTargetReferenceField(TEMP_TARGET_ID_FIELD);
    }

    /**
     * Gets AWS service endpoint.
     * @return AWS service endpoing
     * @since 1.2.0.
     */
    public String getServiceEndpoint() {
        return serviceEndpoint;
    }
    /**
     * Sets AWS service endpoint.
     * @param serviceEndpoint AWS service endpoing
     * @since 1.2.0.
     */
    public void setServiceEndpoint(String serviceEndpoint) {
        this.serviceEndpoint = serviceEndpoint;
        needNewAwsClient = true;
    }
    /**
     * Gets the AWS signing region.
     * @return the AWS signing region
     * @since 1.2.0.
     */
    public String getSigningRegion() {
        return signingRegion;
    }
    /**
     * Gets the AWS signing region.
     * @param signingRegion the AWS signing region
     * @since 1.2.0.
     */
    public void setSigningRegion(String signingRegion) {
        this.signingRegion = signingRegion;
        needNewAwsClient = true;
    }
    /**
     * Gets the CloudSearch document endpoint. 
     * @return document endpoint
     * @deprecated Since 1.2.0, use {@link #setServiceEndpoint(String)}
     */
    @Deprecated
    public String getDocumentEndpoint() {
        return getServiceEndpoint();
    }
    /**
     * Sets the CloudSearch document endpoint.
     * @param documentEndpoint document endpoint
     * @deprecated Since 1.2.0, use {@link #getServiceEndpoint()}
     */
    @Deprecated
    public void setDocumentEndpoint(String documentEndpoint) {
        setServiceEndpoint(documentEndpoint);
    }

    /**
     * Gets the CloudSearch access key. If <code>null</code>, the access key
     * will be obtained from the environment, as detailed in 
     * {@link DefaultAWSCredentialsProviderChain}.
     * @return the access key 
     */
    public String getAccessKey() {
        return accessKey;
    }
    /**
     * Sets the CloudSearch access key.  If <code>null</code>, the access key
     * will be obtained from the environment, as detailed in 
     * {@link DefaultAWSCredentialsProviderChain}.
     * @param accessKey the access key
     */
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        needNewAwsClient = true;
    }

    /**
     * Gets the CloudSearch secret key. If <code>null</code>, the secret key
     * will be obtained from the environment, as detailed in 
     * {@link DefaultAWSCredentialsProviderChain}.
     * @return the secret key 
     */
    public String getSecretKey() {
        return secretKey;
    }
    /**
     * Sets the CloudSearch secret key.  If <code>null</code>, the secret key
     * will be obtained from the environment, as detailed in 
     * {@link DefaultAWSCredentialsProviderChain}.
     * @param secretKey the secret key
     */
    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        needNewAwsClient = true;
    }
    
    /**
     * This method is not supported and will throw an 
     * {@link UnsupportedOperationException} if invoked.  With CloudSearch,
     * the target field for a document unique id is always "id".
     * @param targetReferenceField the target field
     */
    @Override
    public void setTargetReferenceField(String targetReferenceField) {
        if (!TEMP_TARGET_ID_FIELD.equals(targetReferenceField)
                && targetReferenceField != null) {
            LOG.warn("Target reference field is always \"id\" "
                    + "and cannot be changed.");
        }
    }

    /**
     * Gets whether to fix IDs that are too long for CloudSearch
     * ID limitation (128 characters max). If <code>true</code>, 
     * long IDs will be truncated and a hash code representing the 
     * truncated part will be appended.
     * @return <code>true</code> to fix IDs that are too long
     * @since 1.3.0
     */
    public boolean isFixBadIds() {
        return fixBadIds;
    }
    /**
     * Sets whether to fix IDs that are too long for CloudSearch
     * ID limitation (128 characters max). If <code>true</code>, 
     * long IDs will be truncated and a hash code representing the 
     * truncated part will be appended.
     * @param fixBadIds <code>true</code> to fix IDs that are too long
     * @since 1.3.0
     */
    public void setFixBadIds(boolean fixBadIds) {
        this.fixBadIds = fixBadIds;
    }
    
    /**
     * Gets the proxy host.
     * @return proxy host
     * @since 1.4.0
     */
    public String getProxyHost() {
        return proxyHost;
    }
    /**
     * Sets the proxy host.
     * @param proxyHost proxy host
     * @since 1.4.0
     */
    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    /**
     * Gets the proxy port.
     * @return proxy port
     * @since 1.4.0
     */
    public int getProxyPort() {
        return proxyPort;
    }
    /**
     * Sets the proxy port.
     * @param proxyPort proxy port
     * @since 1.4.0
     */
    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    /**
     * Gets the proxy username.
     * @return proxy username
     * @since 1.4.0
     */
    public String getProxyUsername() {
        return proxyUsername;
    }
    /**
     * Sets the proxy username
     * @param proxyUsername proxy username
     * @since 1.4.0
     */
    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
    }

    /**
     * Gets the proxy password.
     * @return proxy password
     * @since 1.4.0
     */
    public String getProxyPassword() {
        return proxyPassword;
    }
    /**
     * Sets the proxy password.
     * @param proxyPassword proxy password
     * @since 1.4.0
     */
    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }
    
    /**
     * Gets the proxy password encryption key.
     * @return the password key or <code>null</code> if the password is not
     * encrypted.
     * @see EncryptionUtil
     * @since 1.4.0
     */
    public EncryptionKey getProxyPasswordKey() {
        return proxyPasswordKey;
    }
    /**
     * Sets the proxy password encryption key. Only required when 
     * the password is encrypted.
     * @param proxyPasswordKey password key
     * @see EncryptionUtil
     * @since 1.4.0
     */
    public void setProxyPasswordKey(EncryptionKey proxyPasswordKey) {
        this.proxyPasswordKey = proxyPasswordKey;
    }
    
    @Override
    protected void commitBatch(List<ICommitOperation> batch) {        
        LOG.info("Sending " + batch.size() 
                + " documents to AWS CloudSearch for addition/deletion.");
        
        List<JSONObject> documentBatch = new ArrayList<>();
        for (ICommitOperation op : batch) {
            if (op instanceof IAddOperation) {
               documentBatch.add(buildJsonDocumentAddition(
                       ((IAddOperation) op).getMetadata()));
            } else if (op instanceof IDeleteOperation) {
                documentBatch.add(buildJsonDocumentDeletion(
                        ((IDeleteOperation) op).getReference()));
            } else {
                throw new CommitterException("Unsupported operation:" + op);
            }
        }

        uploadBatchToCloudSearch(documentBatch);
    }
    
    private void uploadBatchToCloudSearch(List<JSONObject> documentBatch) {
        // Convert the JSON list to String and read it as a stream from memory
        // (for increased performance), for it to be usable by the AWS 
        // CloudSearch UploadRequest. If memory becomes a concern, consider 
        // streaming to file.
        // ArrayList.toString() joins the elements in a JSON-compliant way.
        byte[] bytes = 
                documentBatch.toString().getBytes(StandardCharsets.UTF_8);
        try (ByteArrayInputStream is = new ByteArrayInputStream(bytes)) {
            UploadDocumentsRequest uploadRequest = new UploadDocumentsRequest();
            uploadRequest.setContentType("application/json");
            uploadRequest.setDocuments(is);
            uploadRequest.setContentLength((long) bytes.length);
            ensureAWSClient();
            UploadDocumentsResult result = 
                    awsClient.uploadDocuments(uploadRequest); 
            LOG.info(result.getAdds() + " Add requests and "
                    + result.getDeletes() + " Delete requests "
                    + "sent to the AWS CloudSearch domain."); 
        } catch (IOException | AmazonServiceException e) {
            LOG.error("CloudSearch error: " + e.getMessage());
            throw new CommitterException(
                    "Could not upload request to CloudSearch: "
                            + e.getMessage(), e);
        }
    }
    
    private synchronized void ensureAWSClient() {
        if (StringUtils.isBlank(getServiceEndpoint())) {
            throw new CommitterException("Service endpoint is undefined.");
        }
        
        if (!needNewAwsClient) {
            return;
        }

        AmazonCloudSearchDomainClientBuilder b = 
                AmazonCloudSearchDomainClientBuilder.standard();
        b.setClientConfiguration(buildClientConfiguration());
        if (StringUtils.isAnyBlank(accessKey, secretKey)) {
            b.withCredentials(new DefaultAWSCredentialsProviderChain());
        } else {
            b.withCredentials(new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(accessKey, secretKey)));
        }
        b.withEndpointConfiguration(
                new EndpointConfiguration(serviceEndpoint, signingRegion));
        awsClient = b.build();
        needNewAwsClient = false;
    }

    protected ClientConfiguration buildClientConfiguration(){
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        if (StringUtils.isNotBlank(getProxyHost())) {
            clientConfiguration.setProxyHost(getProxyHost());
            clientConfiguration.setProxyPort(getProxyPort());
            if (StringUtils.isNotBlank(getProxyUsername())) {
                clientConfiguration.setProxyUsername(getProxyUsername());
                clientConfiguration.setProxyPassword(EncryptionUtil.decrypt(
                        proxyPassword, proxyPasswordKey));
            }
        }
        return clientConfiguration;
    }

    private JSONObject buildJsonDocumentAddition(Properties fields) {
    	if (fields.isEmpty()) {
    	    throw new CommitterException(
    	            "Attempting to commit an empty document.");
    	}
    	
        Map<String, Object> documentMap = new HashMap<>();
        documentMap.put("type", "add");
        documentMap.put(COULDSEARCH_ID_FIELD, 
                fixBadIdValue(fields.getString(TEMP_TARGET_ID_FIELD)));
        fields.remove(TEMP_TARGET_ID_FIELD);
        Map<String, Object> fieldMap = new HashMap<>();
        for (String key : fields.keySet()) {
            List<String> values = fields.getStrings(key);
            if (!COULDSEARCH_ID_FIELD.equals(key)) {
                /*size = 1 : non-empty single-valued field
                  size > 1 : non-empty multi-valued field
                  size = 0 : empty field
                */  
                String fixedKey = fixKey(key);
                if (values.size() == 1) {
                    fieldMap.put(fixedKey, values.get(0));
                } else if (values.size() > 1){
                    fieldMap.put(fixedKey, values);
                } else {
                    fieldMap.put(fixedKey, "");
                }
            }
        }    
        documentMap.put("fields", fieldMap);
        return new JSONObject(documentMap);
    }
    
    private String fixBadIdValue(String value) {
        if (StringUtils.isBlank(value)) {
            throw new CommitterException("Document id cannot be empty.");
        }
        
        if (fixBadIds) {
            String v = value.replaceAll(
                    "[^a-zA-Z0-9\\-\\_\\/\\#\\:\\.\\;\\&\\=\\?"
                  + "\\@\\$\\+\\!\\*'\\(\\)\\,\\%]", "_");
            v = StringUtil.truncateWithHash(v, 128, "!");
            if (LOG.isDebugEnabled() && !value.equals(v)) {
                LOG.debug("Fixed document id from \"" + value + "\" to \""
                        + v + "\".");
            }
            return v;
        }
        return value;
    }
    private String fixKey(String key) {
        if (FIELD_PATTERN.matcher(key).matches()) {
            return key;
        }
        String fix = key;
        fix = fix.replaceFirst("^[^a-zA-Z0-9]", "");
        fix = StringUtils.truncate(fix, 63);
        fix = fix.replaceAll("[^a-zA-Z0-9_]", "_");
        fix = fix.toLowerCase(Locale.ENGLISH);
        LOG.warn("\"" + key + "\" field renamed to \"" + fix + "\" as it "
                + "does not match CloudSearch required pattern: " 
                + FIELD_PATTERN);
        return fix;
    }
    
    private JSONObject buildJsonDocumentDeletion(String reference) {
        Map<String, Object> documentMap = new HashMap<>();
        documentMap.put("type", "delete");
        documentMap.put(COULDSEARCH_ID_FIELD, fixBadIdValue(reference));
        return new JSONObject(documentMap);
    }

    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {
        EnhancedXMLStreamWriter w = new EnhancedXMLStreamWriter(writer);
        w.writeElementString("serviceEndpoint", serviceEndpoint);
        w.writeElementString("signingRegion", signingRegion);
        w.writeElementString("accessKey", accessKey);
        w.writeElementString("secretKey", secretKey);
        w.writeElementBoolean("fixBadIds", fixBadIds);
        w.writeElementString("proxyHost", proxyHost);
        w.writeElementInteger("proxyPort", proxyPort);
        w.writeElementString("proxyUsername", proxyUsername);
        w.writeElementString("proxyPassword", proxyPassword);
        saveXMLPasswordKey(w, "proxyPasswordKey", proxyPasswordKey);
    }
    private void saveXMLPasswordKey(EnhancedXMLStreamWriter writer, 
            String field, EncryptionKey key) throws XMLStreamException {
        if (key == null) {
            return;
        }
        writer.writeElementString(field, key.getValue());
        if (key.getSource() != null) {
            writer.writeElementString(
                    field + "Source", key.getSource().name().toLowerCase());
        }
    }

    
    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        String endpoint = xml.getString("serviceEndpoint", null);
        if (StringUtils.isBlank(endpoint)) {
            endpoint = xml.getString("documentEndpoint", null);
            if (StringUtils.isNotBlank(endpoint)) {
                LOG.warn("XML configuration \"documentEndpoint\" is "
                        + "deprecated. Use \"serviceEndpoint\" instead.");
            } else {
                endpoint = getServiceEndpoint();
            }
        }
        setServiceEndpoint(endpoint);
        setSigningRegion(xml.getString("signingRegion", getSigningRegion()));
        setAccessKey(xml.getString("accessKey", getAccessKey()));
        setSecretKey(xml.getString("secretKey", getSecretKey()));
        setFixBadIds(xml.getBoolean("fixBadIds", isFixBadIds()));
        setProxyHost(xml.getString("proxyHost", getProxyHost()));
        setProxyPort(xml.getInt("proxyPort", getProxyPort()));
        setProxyUsername(xml.getString("proxyUsername", getProxyUsername()));
        setProxyPassword(xml.getString("proxyPassword", getProxyPassword()));
        setProxyPasswordKey(
                loadXMLPasswordKey(xml, "proxyPasswordKey", proxyPasswordKey));
    }
    
    private EncryptionKey loadXMLPasswordKey(
            XMLConfiguration xml, String field, EncryptionKey defaultKey) {
        String xmlKey = xml.getString(field, null);
        String xmlSource = xml.getString(field + "Source", null);
        if (StringUtils.isBlank(xmlKey)) {
            return defaultKey;
        }
        EncryptionKey.Source source = null;
        if (StringUtils.isNotBlank(xmlSource)) {
            source = EncryptionKey.Source.valueOf(xmlSource.toUpperCase());
        }
        return new EncryptionKey(xmlKey, source);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .appendSuper(super.hashCode())
            .append(serviceEndpoint)
            .append(signingRegion)
            .append(accessKey)
            .append(secretKey)
            .append(fixBadIds)
            .append(proxyHost)
            .append(proxyPort)
            .append(proxyUsername)
            .append(proxyPassword)
            .append(proxyPasswordKey)
            .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof CloudSearchCommitter)) {
            return false;
        }
        CloudSearchCommitter other = (CloudSearchCommitter) obj;
        return new EqualsBuilder()
            .appendSuper(super.equals(obj))
            .append(serviceEndpoint, other.serviceEndpoint)
            .append(signingRegion, other.signingRegion)
            .append(accessKey, other.accessKey)
            .append(secretKey, other.secretKey)
            .append(fixBadIds, other.fixBadIds)
            .append(proxyHost, proxyHost)
            .append(proxyPort, proxyPort)
            .append(proxyUsername, proxyUsername)
            .append(proxyPassword, proxyPassword)
            .append(proxyPasswordKey, proxyPasswordKey)
            .isEquals();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("serviceEndpoint", serviceEndpoint)
                .append("signingRegion", signingRegion)
                .append("accessKey", accessKey)
                .append("secretKey", secretKey)
                .append("fixBadIds", fixBadIds)
                .append("proxyHost", proxyHost)
                .append("proxyPort", proxyPort)
                .append("proxyUsername", proxyUsername)
                .append("proxyPassword", "*****")
                .append("proxyPasswordKey", proxyPasswordKey)
                .toString();
    }
}
