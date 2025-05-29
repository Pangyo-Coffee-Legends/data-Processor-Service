package com.nhnacademy.dataprocessorservice.config;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * MQTT 클라이언트 설정을 관리하는 구성 클래스입니다.
 * <p>
 * 애플리케이션 시작 시 랜덤 클라이언트 ID를 생성하고, MQTT 브로커와의 연결 옵션을 설정합니다.
 * cleanSession=true 로 이전 세션을 초기화하며, 재연결 시 자동으로 구독을 유지합니다.
 * 종료 시 구독 해제 및 연결 해제를 수행합니다.
 * </p>
 */
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
    private MqttClient client;
    private static final int QOS = 1;

    /**
     * 애플리케이션 시작 시 한 번만 호출되어 랜덤한 클라이언트 ID를 생성합니다.
     */
    @PostConstruct
    public void init() {
        this.clientId = clientIdPrefix + "-" + java.util.UUID.randomUUID().toString().substring(0, 8);
        log.info("MQTT 설정: brokerUrl={}, clientId={}, topic={}", brokerUrl, clientId, topic);
    }

    /**
     * MQTT 연결 옵션을 생성합니다.
     *
     * @return 초기화된 MqttConnectOptions 객체
     */
    @Bean
    public MqttConnectOptions mqttConnectOptions() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setKeepAliveInterval(60);
        options.setConnectionTimeout(30);
        options.setWill("client/status/" + clientId, "offline".getBytes(), QOS, true);
        return options;
    }

    /**
     * MqttClient 빈을 생성하여 브로커에 연결하고 구독을 설정합니다.
     *
     * @param options mqttConnectOptions()에서 생성된 연결 옵션
     * @return 초기화된 MqttClient 객체
     * @throws MqttException 연결 또는 구독 중 오류 발생 시
     */
    @Bean
    public MqttClient mqttClient(MqttConnectOptions options) throws MqttException {
        client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());

        client.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                log.info("MQTT 연결 완료 (reconnect={}): {}", reconnect, serverURI);
                try {
                    client.subscribe(topic, QOS);
                    log.info("MQTT 재구독 완료: {}", topic);
                } catch (MqttException e) {
                    log.error("MQTT 구독 실패: {}", e.getMessage(), e);
                }
            }

            @Override
            public void connectionLost(Throwable cause) {
                log.warn("MQTT 연결 끊김: {}", cause.getMessage());
            }

            @Override
            public void messageArrived(String t, MqttMessage message) {
                log.debug("메시지 도착: topic={}, payload={}", t, new String(message.getPayload()));
            }

            @Override
            public void deliveryComplete(org.eclipse.paho.client.mqttv3.IMqttDeliveryToken token) { }
        });

        client.connect(options);
        client.subscribe(topic, QOS);
        log.info("MQTT 최초 연결 및 구독 완료 → broker={}, topic={}", brokerUrl, topic);
        return client;
    }

    /**
     * 애플리케이션 종료 시 호출되어 MQTT 구독 해제와 연결 종료를 수행합니다.
     *
     * @throws MqttException 해제 또는 연결 종료 중 오류 발생 시
     */
    @PreDestroy
    public void cleanup() throws MqttException {
        if (client != null && client.isConnected()) {
            client.unsubscribe(topic);
            client.disconnect();
            log.info("MQTT 세션 정리 완료");
        }
    }
}