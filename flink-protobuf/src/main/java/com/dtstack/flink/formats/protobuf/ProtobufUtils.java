package com.dtstack.flink.formats.protobuf;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.dtstack.flink.MoreSuppliers;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

public class ProtobufUtils {

    @SuppressWarnings("unchecked")
    public static <M extends Message> M getDefaultInstance(Class<M> messageClazz) {
        try {
            Method getDefaultInstanceMethod = messageClazz.getMethod("getDefaultInstance");
            return (M) getDefaultInstanceMethod.invoke(null);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException var) {
            throw new IllegalArgumentException(var);
        }
    }

    public static Message getDefaultInstance(byte[] descriptorBytes) {
        return DynamicMessage.newBuilder(getDescriptor(descriptorBytes)).getDefaultInstanceForType();
    }

    // example
    public static Descriptors.Descriptor getDescriptor(byte[] descriptorBytes) {
        DescriptorProtos.FileDescriptorSet fileDescriptorSet =
                MoreSuppliers.throwing(() -> DescriptorProtos.FileDescriptorSet.parseFrom(descriptorBytes));
        List<FileDescriptorProto> fileDescriptorProtos = fileDescriptorSet.getFileList();
        Map<String, FileDescriptorProto> protoNameFileDescriptorProtoMapper = fileDescriptorProtos
                .stream()
                .collect(Collectors.toMap(FileDescriptorProto::getName, Function.identity()));
        FileDescriptor fileDescriptor =
                MoreSuppliers.throwing(() ->
                        FileDescriptor.buildFrom(fileDescriptorProtos.get(0), new FileDescriptor[0]));
        return fileDescriptor.getMessageTypes().get(0);
    }

    public static FileDescriptor getFileDescriptor(byte[] descriptorBytes) {
        DescriptorProtos.FileDescriptorSet fileDescriptorSet =
                MoreSuppliers.throwing(() -> DescriptorProtos.FileDescriptorSet.parseFrom(descriptorBytes));
        List<FileDescriptorProto> fileDescriptorProtos = fileDescriptorSet.getFileList();
        Map<String, FileDescriptorProto> protoNameFileDescriptorProtoMapper = fileDescriptorProtos
                .stream()
                .collect(Collectors.toMap(FileDescriptorProto::getName, Function.identity()));
        return MoreSuppliers.throwing(() ->
                FileDescriptor.buildFrom(fileDescriptorProtos.get(0), new FileDescriptor[0]));
    }

    public static Descriptors.Descriptor getDescriptor(Class<? extends Message> messageClazz) {
        return getDefaultInstance(messageClazz).getDescriptorForType();
    }


    public static byte[] getBytes(InputStream is) throws Exception {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = is.read(buffer)) != -1) {
                bos.write(buffer, 0, len);
            }
            is.close();
            bos.flush();
            return bos.toByteArray();
        }
    }

}
