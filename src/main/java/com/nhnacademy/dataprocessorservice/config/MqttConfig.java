package com.nhnacademy.dataprocessorservice.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
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
    private static final int QOS = 1;

    @PostConstruct
    public void init() {
        // 고유한 클라이언트 ID 생성
        this.clientId = clientIdPrefix + "-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("MQTT 설정: brokerUrl={}, clientId={}, topic={}", brokerUrl, clientId, topic);
    }

    @Bean
    public MqttConnectOptions mqttConnectOptions() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);   // 자동 재연결
        options.setCleanSession(false);        // 세션 유지
        options.setKeepAliveInterval(60);      // PING 간격(초)
        options.setConnectionTimeout(30);
        // Last Will 메시지 설정
        options.setWill("client/status/" + clientId, "offline".getBytes(), QOS, true);
        return options;
    }

    @Bean
    public MqttClient mqttClient(MqttConnectOptions options) throws MqttException {
        MqttClient client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());

        // 1) 콜백 설정: 자동 재연결 후 구독 유지
        client.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                log.info("MQTT 연결 완료 (reconnect={}): {}", reconnect, serverURI);
                try {
                    client.subscribe(topic, QOS);
                    log.info("MQTT 구독 완료: {}", topic);
                } catch (MqttException e) {
                    log.error("MQTT 구독 실패: {}", e.getMessage(), e);
                }
            }

            @Override
            public void connectionLost(Throwable cause) {
                log.warn("MQTT 연결 끊김: {}", cause.getMessage());
            }

            @Override
            public void messageArrived(String t, MqttMessage message) throws Exception {
                // 실제 메시지 처리 로직은 MqttSubscriberService에서 처리하므로 빈 간단히 로깅만
                log.debug("메시지 도착: topic={}, payload={}", t, new String(message.getPayload()));
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) { }
        });

        // 2) 최초 연결 및 구독
        client.connect(options);
        client.subscribe(topic, QOS);
        log.info("MQTT 최초 연결 및 구독 완료 → broker={}, topic={}", brokerUrl, topic);

        return client;
    }
}
