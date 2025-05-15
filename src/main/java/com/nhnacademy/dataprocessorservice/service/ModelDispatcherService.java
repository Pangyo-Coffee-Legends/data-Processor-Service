package com.nhnacademy.dataprocessorservice.service;

import com.nhnacademy.traceloggermodule.logging.FlowLogger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
@Slf4j
@Service
@RequiredArgsConstructor
public class ModelDispatcherService {
    private static final Set<String> MODEL_FIELDS = Set.of("temperature","humidity","co2");
    private final RestTemplate restTemplate;

    @Value("${model.api-url}")
    private String modelApiUrl;

    @Async("modelDispatcherExecutor")
    public void dispatch(String location, String sensorType, double value) {
        if (!MODEL_FIELDS.contains(sensorType)) return;

        String traceId = UUID.randomUUID().toString();
        String source = "data-processor-service";
        String messageId = UUID.randomUUID().toString();

        MDC.put("traceId", traceId);
        MDC.put("source", source);
        MDC.put("messageId", messageId);

        Map<String, Object> payload = Map.of(
                "location", location,
                "sensor_type", sensorType,
                "value", value
        );

        try {
            FlowLogger.log("ModelDispatcherService#dispatch", payload);

            restTemplate.postForEntity(modelApiUrl, payload, Void.class);
            log.info("✅ 모델 서비스 전송 성공: {}", payload);

        } catch (HttpClientErrorException e) {
            log.error("❌ 모델 서비스 4xx 에러({}): {} | payload={}",
                    e.getStatusCode(), e.getResponseBodyAsString(), payload);
        } catch (RuntimeException e) {
            log.error("❌ 모델 서비스 호출 실패: {} | payload={}", e.getMessage(), payload);
        } finally {
            MDC.clear();
        }
    }
}
