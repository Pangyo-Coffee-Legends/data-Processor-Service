package com.nhnacademy.dataprocessorservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nhnacademy.dataprocessorservice.dto.SensorDataDto;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class MqttSubscriberService {

    private final MqttClient mqttClient;
    private final MqttConnectOptions mqttConnectOptions;
    private final InfluxService influxService; // 추가

    @Value("${mqtt.topic}")
    private String mqttTopic;
    private static final int QOS = 1;

    @PostConstruct
    public void subscribe() {
        try {
            if (!mqttClient.isConnected()) {
                mqttClient.connect(mqttConnectOptions);
            }

            // 1. 환경변수에서 토픽 분리
            String[] topics = mqttTopic.split(",");

            // 2. 각 토픽 구독
            for (String topic : topics) {
                mqttClient.subscribe(topic.trim(), QOS, (receivedTopic, message) -> {
                    String payload = new String(message.getPayload());
                    processMessage(receivedTopic, payload);
                });
            }

            log.info("🚀 MQTT 브로커 연결 성공. 구독 토픽: {}", Arrays.toString(topics));

        } catch (MqttException e) {
            log.error("❌ MQTT 구독 실패", e);
        }
    }


    // 메시지 처리 메서드 (추가)
    private void processMessage(String topic, String payload) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            SensorDataDto rawDto = mapper.readValue(payload, SensorDataDto.class);

            // 1. 토픽에서 위치와 센서 종류 추출
            String[] topicParts = topic.split("/");
            String location = topicParts[topicParts.length - 3]; // "/n/{위치}/e/{센서}"
            String sensorType = topicParts[topicParts.length - 1];

            // [추가] LoRa 센서는 처리하지 않음
            if ("lora".equalsIgnoreCase(sensorType)) {
                log.debug("🚫 LoRa 통신 데이터 제외 | 토픽: {}", topic);
                return;
            }

            // 2. 센서 종류 매핑
            String sensorName = switch (sensorType) {
                case "temperature" -> "온도";
                case "humidity" -> "습도";
                case "co2" -> "이산화탄소";
                case "battery" -> "배터리";
                case "illumination" -> "조도";
                default -> sensorType; // 알 수 없는 센서
            };

            String unit = switch (sensorType) {
                case "temperature" -> "℃";
                case "humidity" -> "%";
                case "co2" -> "ppm";
                case "battery" -> "%";
                case "illumination" -> "Lux";
                default -> "-";
            };

            // 3. 시간 포맷팅
            String formattedTime = Instant.ofEpochMilli(rawDto.getTime())
                    .atZone(ZoneId.of("Asia/Seoul"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            // 👉 [수정된 안전한 센서 값 추출 로직 시작]
            Object valueObj = rawDto.getValue();
            Double sensorValue;

            if (valueObj instanceof Integer intVal) {
                sensorValue = intVal.doubleValue();
            } else if (valueObj instanceof Double doubleVal) {
                sensorValue = doubleVal;
            } else if (valueObj instanceof LinkedHashMap<?, ?> mapVal) {
                Object innerValue = mapVal.get(sensorType);
                if (innerValue instanceof Number numberVal) {
                    sensorValue = numberVal.doubleValue();
                } else {
                    log.warn("⚠️ 내부 값이 숫자가 아님: {}", innerValue);
                    return;
                }
            } else {
                log.warn("⚠️ 예외적인 value 타입: {}", valueObj.getClass());
                return;
            }
            // 👈 [수정된 안전한 센서 값 추출 로직 끝]

            // 4. 로그 출력
            log.info("📍 위치: {} | ⏰ 시간: {} | 🔍 센서: {} | 📊 값: {} {}",
                    location, formattedTime, sensorName, sensorValue, unit);

            // 5. InfluxDB 저장
            influxService.writeSensorData(location, sensorType, sensorValue);


        } catch (Exception e) {
            log.error("❌ JSON 파싱 실패: {}", e.getMessage());
        }
    }
}
