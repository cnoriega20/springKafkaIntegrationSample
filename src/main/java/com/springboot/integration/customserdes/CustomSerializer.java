package com.springboot.integration.customserdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.springboot.integration.domain.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class CustomSerializer implements Serializer<Employee> {

    private ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Employee employee) {
        try{
            if (employee == null){
                log.info("Null received at serializing");
                return null;
            }
            log.info("Serializing...");
            return objectMapper.writeValueAsBytes(employee);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Employee to byte[]");
        }

    }

    @Override
    public byte[] serialize(String topic, Headers headers, Employee data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
