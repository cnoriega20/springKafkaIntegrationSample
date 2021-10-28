package com.springboot.integration.config;

import com.springboot.integration.domain.Employee;
import com.springboot.integration.handlers.CountDownLatchHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ConsumingChannelConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.spring-integration-kafka}")
    private String springIntegrationKafkaTopic;

    @Bean
    public DirectChannel consumingChannel() {
        return new DirectChannel();
    }

    @Bean
    public KafkaMessageDrivenChannelAdapter<String, Employee> kafkaMessageDrivenChannelAdapter() {
        KafkaMessageDrivenChannelAdapter<String, Employee> kafkaMessageDrivenChannelAdapter =
                new KafkaMessageDrivenChannelAdapter<>(kafkaListenerContainer());
        kafkaMessageDrivenChannelAdapter.setOutputChannel(consumingChannel());

        return kafkaMessageDrivenChannelAdapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "consumingChannel")
    public CountDownLatchHandler countDownLatchHandler() {
        return new CountDownLatchHandler();
    }

    @SuppressWarnings("unchecked")
    @Bean
    public ConcurrentMessageListenerContainer<String, Employee> kafkaListenerContainer() {
        ContainerProperties containerProps = new ContainerProperties(springIntegrationKafkaTopic);

        return new ConcurrentMessageListenerContainer<>((ConsumerFactory<? super String, ? super Employee>) consumerFactory(), containerProps);
    }

    @Bean
    public ConsumerFactory<?, ?> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "spring-integration");
        // automatically reset the offset to the earliest offset
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }
}
