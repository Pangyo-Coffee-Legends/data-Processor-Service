package com.nhnacademy.dataprocessorservice.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class InfluxService {

    private final InfluxDBClient influxDBClient;

    public void writeSensorData(String location, String sensorType, double value) {
        Point point = Point.measurement("sensor")
                .addTag("location", location)
                .addTag("type", sensorType)
                .addField("value", value)
                .time(System.currentTimeMillis(), WritePrecision.MS);

        try (WriteApi writeApi = influxDBClient.getWriteApi()) {
            writeApi.writePoint(point);
        }
    }
}
