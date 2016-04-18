/* Copyright 2016 Norconex Inc.
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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomainClient;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsRequest;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsResult;
import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.map.Properties;

/**
 * <p>
 * Commits documents to Amazon CloudSearch.
 * </p>
 * <h3>Authentication:</h3>
 * An access key and security key are required to connect to interact with 
 * CloudSearch. For enhanced security, it is best to use one of the methods
 * described in {@link DefaultAWSCredentialsProviderChain} for setting them
 * (environment variables, system properties, profile file, etc). 
 * Do not explicitely set "accessKey" and "secretKey" on this class relying on
 * safer methods.
 * 
 * <h3>XML configuration usage:</h3>
 * 
 * <pre>
 *  &lt;committer class="com.norconex.committer.cloudsearch.CloudSearchCommitter"&gt;
 *      &lt;documentEndpoint&gt;(CloudSearch document endpoint)&lt;/documentEndpoint&gt;
 *      &lt;accessKey&gt;
 *         (Optional CloudSearch access key. Will be taken from environment 
 *          when blank.)
 *      &lt;/accessKey&gt;
 *      &lt;secretKey&gt;
 *         (Optional CloudSearch secret key. Will be taken from environment
 *          when blank.)
 *      &lt;/secretKey&gt;
 *      
 *      &lt;sourceReferenceField keep="[false|true]"&gt;
 *         (Optional name of field that contains the document reference, when 
 *         the default document reference is not used.  The reference value
 *         will be mapped to CloudSearch "id" field, or the 
 *         "targetReferenceField" specified.
 *         Once re-mapped, this metadata source field is 
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceReferenceField&gt;
 *      &lt;targetReferenceField&gt;
 *         (Name of CloudSearch target field where the store a document unique 
 *         identifier (idSourceField).  If not specified, default is "id".) 
 *      &lt;/targetReferenceField&gt;
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
 *          (Max delay between retries. Default is 0.)
 *      &lt;/maxRetryWait&gt;
 *  &lt;/committer&gt;
 * </pre>
 * 
 * @author El-Hebri Khiari
 * @author Pascal Essiembre
 */
public class CloudSearchCommitter extends AbstractMappedCommitter {

    private static final Logger LOG = 
            LogManager.getLogger(CloudSearchCommitter.class);
    
    /** Default CloudSearch ID field */
    public static final String DEFAULT_COULDSEARCH_ID_FIELD = "id";
    /** Default CloudSearch content field */
    public static final String DEFAULT_COULDSEARCH_CONTENT_FIELD = "content";
    
    
    private AmazonCloudSearchDomainClient awsClient;
    private boolean needNewAwsClient = true;
    
    private String documentEndpoint;
    private String accessKey;
    private String secretKey;
    
    public CloudSearchCommitter() {
        this(null);
    }
    
    public CloudSearchCommitter(String documentEndpoint) {
        super();
        this.documentEndpoint = documentEndpoint;
        setTargetContentField(DEFAULT_COULDSEARCH_CONTENT_FIELD);
        setTargetReferenceField(DEFAULT_COULDSEARCH_ID_FIELD);
    }
    
    /**
     * Gets the CloudSearch document endpoint. 
     * @return document endpoint
     */
    public String getDocumentEndpoint() {
        return documentEndpoint;
    }
    /**
     * Sets the CloudSearch document endpoint.
     * @param documentEndpoint document endpoint
     */
    public void setDocumentEndpoint(String documentEndpoint) {
        this.documentEndpoint = documentEndpoint;
        needNewAwsClient = true;
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

    @Override
    protected void commitBatch(List<ICommitOperation> batch) {        
        LOG.info("Sending " + batch.size() 
                + " documents to AWS CloudSearch for addition/deletion.");
        
        List<JSONObject> documentBatch = new ArrayList<>();
        for (ICommitOperation op : batch) {
            if (op instanceof IAddOperation) {
               documentBatch.add(buildJsonDocumentAddition(
                       ((IAddOperation) op).getMetadata(),
                       ((IAddOperation) op).getReference()));
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
        byte[] bytes;
        try {
            bytes = documentBatch.toString().getBytes(CharEncoding.UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new CommitterException("UTF-8 not supported by OS.", e);
        }
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
        } catch (IOException e) {
            throw new CommitterException(
                    "Could not upload request to CloudSearch.", e);
        }
    }
    
    private synchronized void ensureAWSClient() {
        if (StringUtils.isBlank(getDocumentEndpoint())) {
            throw new CommitterException("Document endpoint is undefined.");
        }
        
        if (!needNewAwsClient) {
            return;
        }
        if (StringUtils.isAnyBlank(accessKey, secretKey)) {
            awsClient = new AmazonCloudSearchDomainClient(
                    new DefaultAWSCredentialsProviderChain());
        } else {
            awsClient = new AmazonCloudSearchDomainClient(
                    new BasicAWSCredentials(accessKey, secretKey));
        }
        awsClient.setEndpoint(documentEndpoint);
        needNewAwsClient = false;
    }

    private JSONObject buildJsonDocumentAddition(
            Properties fields, String reference) {
        Map<String, Object> documentMap = new HashMap<>();
        documentMap.put("type", "add");
        documentMap.put("id", reference);
        Map<String, Object> fieldMap = new HashMap<>();
        for (String key : fields.keySet()) {
            List<String> values = fields.getStrings(key);
            if (!StringUtils.equals(key, "id")) {
                /*size = 1 : non-empty single-valued field
                  size > 1 : non-empty multi-valued field
                  size = 0 : empty field
                */  
                if (values.size() == 1) {
                    fieldMap.put(key, values.get(0));
                } else if (values.size() > 1){
                    fieldMap.put(key, values);
                } else {
                    fieldMap.put(key, "");
                }
            }
        }    
        documentMap.put("fields", fieldMap);
        return new JSONObject(documentMap);
    }
    private JSONObject buildJsonDocumentDeletion(String reference) {
        Map<String, Object> documentMap = new HashMap<>();
        documentMap.put("type", "delete");
        documentMap.put("id", reference);
        return new JSONObject(documentMap);
    }

    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartElement("documentEndpoint");
        writer.writeCharacters(documentEndpoint);
        writer.writeEndElement();

        writer.writeStartElement("accessKey");
        writer.writeCharacters(accessKey);
        writer.writeEndElement();

        writer.writeStartElement("secretKey");
        writer.writeCharacters(secretKey);
        writer.writeEndElement();
    }

    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        setDocumentEndpoint(xml.getString(
                "documentEndpoint", getDocumentEndpoint()));
        setAccessKey(xml.getString("accessKey", getAccessKey()));
        setSecretKey(xml.getString("secretKey", getSecretKey()));
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .appendSuper(super.hashCode())
            .append(documentEndpoint)
            .append(accessKey)
            .append(secretKey)
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
            .append(documentEndpoint, other.documentEndpoint)
            .append(accessKey, other.accessKey)
            .append(secretKey, other.secretKey)
            .isEquals();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("documentEndpoint", documentEndpoint)
                .append("accessKey", accessKey)
                .append("secretKey", secretKey)
                .toString();
    }
}
