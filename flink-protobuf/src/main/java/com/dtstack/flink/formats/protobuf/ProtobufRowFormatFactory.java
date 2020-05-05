package com.dtstack.flink.formats.protobuf;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.dtstack.flink.MoreRunnables;
import com.dtstack.flink.MoreSuppliers;
import com.dtstack.flink.table.descriptors.ProtobufValidator;
import com.google.protobuf.GeneratedMessageV3;

/**
 * Table format factory for providing configured instances of Protobuf-to-row {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
public class ProtobufRowFormatFactory extends TableFormatFactoryBase<Row>
        implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {

    public ProtobufRowFormatFactory() {
        super(ProtobufValidator.FORMAT_TYPE_VALUE, 1, false);
    }

    @Override
    protected List<String> supportedFormatProperties() {
        List<String> properties = new ArrayList<>(2);
        properties.add(ProtobufValidator.FORMAT_TYPE_VALUE);
        properties.add(ProtobufValidator.FORMAT_PROTOBUF_DESCRIPTOR_HTTP_GET_URL);
        return properties;
    }

    @Override
    public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        // create and configure
        if (descriptorProperties.containsKey(ProtobufValidator.FORMAT_MESSAGE_CLASS)) {
            return new ProtobufRowDeserializationSchema(
                    descriptorProperties.getClass(ProtobufValidator.FORMAT_MESSAGE_CLASS, GeneratedMessageV3.class));
        } else {

            String descriptorHttpGetUrl = descriptorProperties.getString(ProtobufValidator.FORMAT_PROTOBUF_DESCRIPTOR_HTTP_GET_URL);

            byte[] descriptorBytes = httpGetDescriptorBytes(descriptorHttpGetUrl);

            return new ProtobufRowDeserializationSchema(descriptorBytes);
        }
    }

    @Override
    public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        // create and configure
        if (descriptorProperties.containsKey(ProtobufValidator.FORMAT_MESSAGE_CLASS)) {
            return new ProtobufRowSerializationSchema(
                    descriptorProperties.getClass(ProtobufValidator.FORMAT_MESSAGE_CLASS, GeneratedMessageV3.class));
        } else {

            String descriptorHttpGetUrl = descriptorProperties.getString(ProtobufValidator.FORMAT_PROTOBUF_DESCRIPTOR_HTTP_GET_URL);

            byte[] descriptorBytes = httpGetDescriptorBytes(descriptorHttpGetUrl);

            return new ProtobufRowSerializationSchema(descriptorBytes);
        }
    }

    public static byte[] httpGetDescriptorBytes(final String descriptorHttpGetUrl) {
        byte[] descriptorBytes = null;

        HttpGet get = new HttpGet(descriptorHttpGetUrl);

        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse httpResponse = MoreSuppliers.throwing(() -> httpClient.execute(get));
        if (200 == httpResponse.getStatusLine().getStatusCode()) {

            long length = httpResponse.getEntity().getContentLength();
            byte[] buffer = new byte[(int) length];

            InputStream is = MoreSuppliers.throwing(() -> httpResponse.getEntity().getContent());
            MoreSuppliers.throwing(() -> is.read(buffer));
            descriptorBytes = buffer;

            MoreRunnables.throwing(is::close);
        }
        MoreRunnables.throwing(httpResponse::close);
        MoreRunnables.throwing(httpClient::close);

        if (null != descriptorBytes && 0 != descriptorBytes.length) {
            return descriptorBytes;
        } else {
            throw new RuntimeException(String.format("Try to get Protobuf descriptorBytes http response by [%s], find null or empty descriptorBytes, please check you descriptorBytes", descriptorHttpGetUrl));
        }
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(propertiesMap);

        // validate
        (new ProtobufValidator()).validate(descriptorProperties);

        return descriptorProperties;
    }
}
