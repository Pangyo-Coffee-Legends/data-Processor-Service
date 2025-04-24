package com.nhnacademy.dataprocessorservice.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Slf4j
@Configuration
public class MqttConfig {
    @Value("${mqtt.broker.url}")
    private String brokerUrl;

    @Value("${mqtt.client.id}")
    private String clientIdPrefix;

    @Value("${mqtt.topic}")
    private String topic;

    private String clientId;

    @PostConstruct
    public void init() {
        // 고유한 클라이언트 ID 생성
        this.clientId = clientIdPrefix + "-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("brokerUrl={}, clientId={}, topic={}", brokerUrl, clientId, topic);
    }

    @Bean
    public MqttClient mqttClient() throws MqttException {
        return new MqttClient(brokerUrl, clientId, new MemoryPersistence());
    }

    @Bean
    public MqttConnectOptions mqttConnectOptions() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(false); // 영구 세션 사용
        options.setKeepAliveInterval(180); // 3분으로 늘림
        options.setConnectionTimeout(30); // 연결 타임아웃 30초
        options.setMaxInflight(100); // 동시 메시지 처리 수 증가

        // Last Will and Testament 설정
        options.setWill("client/status/" + clientId, "offline".getBytes(), 1, true);

        return options;
    }
}
