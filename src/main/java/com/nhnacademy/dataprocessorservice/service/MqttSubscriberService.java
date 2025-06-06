package com.nhnacademy.dataprocessorservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nhnacademy.dataprocessorservice.dto.SensorDataDto;
import com.nhnacademy.dataprocessorservice.exception.InvalidPayloadException;
import com.nhnacademy.dataprocessorservice.exception.MqttProcessingException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.UUID;

/**
 * MQTT 메시지를 구독하고 처리하는 서비스입니다.
 * AOP를 통해 traceId와 response_time을 자동 로깅합니다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MqttSubscriberService {

    private final MqttClient mqttClient;
    private final InfluxService influxService;
    private final ModelDispatcherService dispatcher;
    private final ObjectMapper objectMapper;

    @Value("${mqtt.topic}")
    private String mqttTopic;

    @Value("${spring.application.name}")
    private String serviceName; // source 필드로 사용

    private static final int QOS = 1;
    private long lastMessageReceived;

    /**
     * 애플리케이션 시작 시 구독을 설정합니다.
     */
    @PostConstruct
    public void subscribe() {
        lastMessageReceived = System.currentTimeMillis();
        connectAndSubscribe();
    }

    private void connectAndSubscribe() {
        try {
            if (!mqttClient.isConnected()) {
                mqttClient.connect();
                log.info("🔌 MQTT 연결됨: {}", mqttClient.getServerURI());
            }

            String[] topics = mqttTopic.split(",");
            for (String topic : topics) {
                mqttClient.subscribe(topic.trim(), QOS, (t, msg) -> {
                    try {
                        // ✅ Trace 정보 수동 주입
                        MDC.put("traceId", UUID.randomUUID().toString());
                        MDC.put("source", serviceName);
                        MDC.put("messageId", UUID.randomUUID().toString());

                        String payload = new String(msg.getPayload());
                        // ✅ 프록시 호출 말고 그냥 직접 처리
                        processMessage(t, payload);

                    } catch (Exception e) {
                        log.error("🔥 메시지 처리 실패", e);
                    } finally {
                        MDC.clear(); // ✅ 깨끗하게 정리
                    }
                });
            }

            log.info("🚀 MQTT 구독 완료: {}", Arrays.toString(topics));

        } catch (MqttException e) {
            log.error("❌ MQTT 구독 실패", e);
        }
    }


    /**
     * 메시지를 처리하고 AOP 어드바이스를 트리거합니다.
     *
     * @param topic   MQTT 토픽
     * @param payload 메시지 페이로드 JSON
     */
    public void processMessage(String topic, String payload) {
        MDC.put("messageId", UUID.randomUUID().toString());

        try {
            log.info("📩 수신: topic={} | payload={}", topic, payload);

            SensorDataDto dto = parsePayload(payload);
            String location = extractLocation(topic);
            String sensorType = extractType(topic);

            if (sensorType == null) {
                return;
            }

            double value = extractValue(dto, sensorType);
            String formattedTime = formatTime(dto.getTime());

            log.info("📍 위치: {} | ⏰ 시간: {} | 🔍 센서: {}({}) | 📊 값: {} {}",
                    location, formattedTime,
                    mapName(sensorType), sensorType, value, mapUnit(sensorType));

            influxService.writeSensorData(location, sensorType, value);
            dispatcher.dispatch(location, sensorType, value);
        } catch (Exception e) {
            log.error("🌐 메시지 처리 오류", e);
            throw new MqttProcessingException(e.getMessage());
        } finally {
            MDC.clear();
        }
    }


    private SensorDataDto parsePayload(String payload) {
        try {
            return objectMapper.readValue(payload, SensorDataDto.class);
        } catch (Exception e) {
            throw new InvalidPayloadException("JSON 파싱 실패: " + e.getMessage());
        }
    }

    private String extractLocation(String topic) {
        String[] parts = topic.split("/");
        if (parts.length < 3) throw new InvalidPayloadException("토픽 형식 오류: " + topic);
        return parts[parts.length - 3];
    }

    private String extractType(String topic) {
        String type = topic.substring(topic.lastIndexOf('/') + 1);
        if (!Set.of("temperature", "humidity", "co2", "battery", "illumination").contains(type)) {
            return null; // 또는 throw 예외
        }
        return type;
    }


    private double extractValue(SensorDataDto dto, String type) {
        Object v = dto.getValue();
        if (v instanceof Number) return ((Number) v).doubleValue();
        if (v instanceof LinkedHashMap) {
            Object inner = ((LinkedHashMap<?, ?>) v).get(type);
            if (inner instanceof Number) return ((Number) inner).doubleValue();
        }
        throw new InvalidPayloadException("지원되지 않는 value 타입");
    }

    private String formatTime(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis)
                .atZone(ZoneId.of("Asia/Seoul"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    private String mapName(String type) {
        return switch (type) {
            case "temperature" -> "온도";
            case "humidity" -> "습도";
            case "co2" -> "이산화탄소";
            case "battery" -> "배터리";
            case "illumination" -> "조도";
            default -> type;
        };
    }

    private String mapUnit(String type) {
        return switch (type) {
            case "temperature" -> "℃";
            case "humidity", "battery" -> "%";
            case "co2" -> "ppm";
            case "illumination" -> "Lux";
            default -> "";
        };
    }

    /**
     * 애플리케이션 종료 시 MQTT 연결 정리
     */
    @PreDestroy
    public void destroy() {
        try {
            if (mqttClient.isConnected()) {
                mqttClient.unsubscribe(mqttTopic);
                mqttClient.disconnect();
                log.info("MQTT 세션 종료 완료");
            }
        } catch (MqttException e) {
            log.error("MQTT 정리 중 오류", e);
        }
    }
}
