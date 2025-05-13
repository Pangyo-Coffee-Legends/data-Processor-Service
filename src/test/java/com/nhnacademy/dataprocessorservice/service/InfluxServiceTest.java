package com.nhnacademy.dataprocessorservice.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.*;

class InfluxServiceTest {
    @Test
    void writeSensorData_writesCorrectPoint() {
        WriteApi writeApi = mock(WriteApi.class);
        InfluxService svc = new InfluxService(writeApi);

        svc.writeSensorData("roomA", "temperature", 23.5);

        ArgumentCaptor<Point> captor = ArgumentCaptor.forClass(Point.class);
        verify(writeApi).writePoint(captor.capture());
        Point p = captor.getValue();

        String line = p.toLineProtocol();
        assertTrue(line.startsWith("sensor,"));                        // measurement
        assertTrue(line.contains("location=roomA"));                    // tag location
        assertTrue(line.contains("type=temperature"));                  // tag type
        assertTrue(line.contains("value=23.5"));                        // field value
    }
}