package com.nhnacademy.dataprocessorservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nhnacademy.dataprocessorservice.dto.SensorDataDto;
import com.nhnacademy.dataprocessorservice.exception.InvalidPayloadException;
import com.nhnacademy.dataprocessorservice.exception.MqttProcessingException;
import com.nhnacademy.dataprocessorservice.exception.UnsupportedSensorTypeException;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class MqttSubscriberService {

    private final MqttClient mqttClient;
    private final MqttConnectOptions mqttConnectOptions;
    private final InfluxService influxService;
    private final ModelDispatcherService dispatcher;
    private final ObjectMapper objectMapper;

    @Value("${mqtt.topic}")
    private String mqttTopic;
    private static final int QOS = 1;

    // 마지막 메시지 수신 시간 추적
    private long lastMessageReceived = System.currentTimeMillis();

    @PostConstruct
    public void subscribe() {
        connectAndSubscribe();
    }

    private void connectAndSubscribe() {
        try {
            if (!mqttClient.isConnected()) {
                mqttClient.connect(mqttConnectOptions);
                log.info("🔌 MQTT 브로커에 연결되었습니다: {}", mqttClient.getServerURI());

                // 연결 성공 후 상태 메시지 발행
                mqttClient.publish("client/status/" + mqttClient.getClientId(),
                        "online".getBytes(), 1, true);
            }

            // 토픽 분리 및 구독
            String[] topics = mqttTopic.split(",");
            for (String topic : topics) {
                mqttClient.subscribe(topic.trim(), QOS, (receivedTopic, message) -> {
                    // 메시지 수신 시간 업데이트
                    lastMessageReceived = System.currentTimeMillis();
                    String payload = new String(message.getPayload());
                    processMessage(receivedTopic, payload);
                });
            }

            log.info("🚀 MQTT 브로커 연결 성공. 구독 토픽: {}", Arrays.toString(topics));

        } catch (MqttException e) {
            log.error("❌ MQTT 구독 실패: {}", e.getMessage(), e);
        }
    }

    // 주기적으로 연결 상태 확인 (30초마다)
    @Scheduled(fixedDelay = 30000)
    public void checkConnection() {
        try {
            // 연결이 끊어진 경우 재연결
            if (!mqttClient.isConnected()) {
                log.warn("⚠️ MQTT 연결이 끊어졌습니다. 재연결 시도 중...");
                connectAndSubscribe();
                return;
            }

            // 마지막 메시지 수신 후 2분 이상 지났는지 확인
            long now = System.currentTimeMillis();
            if (now - lastMessageReceived > 120000) {
                log.warn("⚠️ 2분 이상 메시지가 수신되지 않았습니다. 연결 상태 확인 중...");

                // PING 메시지 전송으로 연결 확인
                if (mqttClient.isConnected()) {
                    mqttClient.publish("client/ping/" + mqttClient.getClientId(),
                            "ping".getBytes(), 0, false);
                    log.info("🔄 PING 메시지 전송 완료");
                } else {
                    log.warn("🔌 연결이 끊어졌습니다. 재연결 시도 중...");
                    connectAndSubscribe();
                }
            }
        } catch (MqttException e) {
            log.error("❌ MQTT 연결 확인 중 오류 발생: {}", e.getMessage(), e);
            try {
                // 연결 관련 예외 발생 시 재연결 시도
                mqttClient.disconnectForcibly();
                Thread.sleep(1000);
                connectAndSubscribe();
            } catch (Exception ex) {
                log.error("❌ 강제 재연결 실패: {}", ex.getMessage(), ex);
            }
        }

    }


private void processMessage(String topic, String payload) {
    String messageId = UUID.randomUUID().toString();
    MDC.put("messageId", messageId);

    try {
        log.info("📩 수신된 토픽: {} | 페이로드: {}", topic, payload);

        SensorDataDto rawDto;
        try {
            rawDto = objectMapper.readValue(payload, SensorDataDto.class);
        } catch (Exception e) {
            throw new InvalidPayloadException("JSON 파싱 실패: " + e.getMessage());
        }

        // 토픽 구조 검증 및 위치/센서 타입 추출
        String[] parts = topic.split("/");
        if (parts.length < 3) {
            throw new InvalidPayloadException("토픽 형식 오류: " + topic);
        }
        String location = parts[parts.length - 3];
        String sensorType = parts[parts.length - 1];

        // 지원 센서 타입 확인
        switch (sensorType) {
            case "temperature", "humidity", "co2", "battery", "illumination" -> {
            }
            default -> throw new UnsupportedSensorTypeException(sensorType);
        }

        // 센서명 및 단위 매핑
        String sensorName = switch (sensorType) {
            case "temperature" -> "온도";
            case "humidity"    -> "습도";
            case "co2"         -> "이산화탄소";
            case "battery"     -> "배터리";
            case "illumination"-> "조도";
            default             -> sensorType;
        };
        String unit = switch (sensorType) {
            case "temperature" -> "℃";
            case "humidity"    -> "%";
            case "co2"         -> "ppm";
            case "battery"     -> "%";
            case "illumination"-> "Lux";
            default             -> "-";
        };

        // 값 추출
        Object valueObj = rawDto.getValue();
        Double sensorValue;
        if (valueObj instanceof Number num) {
            sensorValue = num.doubleValue();
        } else if (valueObj instanceof LinkedHashMap<?, ?> map) {
            Object inner = map.get(sensorType);
            if (inner instanceof Number n) {
                sensorValue = n.doubleValue();
            } else {
                throw new InvalidPayloadException("맵 내부 값이 숫자가 아님: " + inner);
            }
        } else {
            throw new InvalidPayloadException("지원하지 않는 value 타입: " + valueObj.getClass());
        }

        // 시간 포맷
        String formatted = Instant.ofEpochMilli(rawDto.getTime())
                .atZone(ZoneId.of("Asia/Seoul"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        log.info("📍 위치: {} | ⏰ 시간: {} | 🔍 센서: {}({}) | 📊 값: {} {}",
                location, formatted, sensorName, sensorType, sensorValue, unit);

        // InfluxDB 저장 및 모델 전송
        try {
            influxService.writeSensorData(location, sensorType, sensorValue);
            dispatcher.dispatch(location, sensorType, sensorValue);
        } catch (Exception e) {
            throw new MqttProcessingException("데이터 처리 중 오류 발생"+e);
        }

    } finally {
        MDC.clear();
    }
}

}