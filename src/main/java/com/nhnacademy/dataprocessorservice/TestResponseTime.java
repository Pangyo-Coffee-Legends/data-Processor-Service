package com.nhnacademy.dataprocessorservice;

import net.logstash.logback.argument.StructuredArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Map;

public class TestResponseTime {
    private static final Logger log = LoggerFactory.getLogger(TestResponseTime.class);

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        MDC.put("traceId", "test-trace-123");
        MDC.put("source", "data-processor-service");

        // 비즈니스 로직 흉내
        try { Thread.sleep(50); } catch (InterruptedException ignored) {}

        long rt = System.currentTimeMillis() - start;
        Map<String,Object> m = Map.of(
                "traceId",       MDC.get("traceId"),
                "source",        MDC.get("source"),
                "target",        "TestResponseTime#main",
                "time",          start,
                "response_time", rt
        );

        // StructuredArguments.entries() 로 JSON 필드로 찍힘
        log.info("{}", StructuredArguments.entries(m));
    }
}
