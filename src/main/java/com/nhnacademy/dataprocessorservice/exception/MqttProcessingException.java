package com.nhnacademy.dataprocessorservice.exception;

public class MqttProcessingException extends RuntimeException {
    public MqttProcessingException(String message) {
        super(message);
    }
}
