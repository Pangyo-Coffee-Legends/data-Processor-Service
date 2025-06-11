# Data Processor Service

이 프로젝트는 MQTT로 수집한 센서 데이터를 가공하여 InfluxDB에 저장하고,
필요 시 모델 서비스로 전달하는 Spring Boot 애플리케이션입니다. Java 21과
Spring Cloud 기술 스택을 사용합니다.

## 주요 기능
- MQTT 브로커로부터 센서 데이터를 구독하여 수신
- 수신한 데이터를 파싱 후 InfluxDB에 기록
- 온도/습도/CO2 센서 데이터는 별도의 모델 서비스로 비동기 전송
- Spring Cloud Config, Eureka Client를 통한 마이크로서비스 환경 지원

## 빌드 및 실행
```bash
# 의존성이 설치된 환경에서 실행
./mvnw spring-boot:run
```

테스트는 `mvn test`로 수행할 수 있습니다. (현재 환경에서는 Maven이 없으면 실행되지 않습니다.)

## 설정
애플리케이션 설정은 Spring Cloud Config 서버에서 로드하며,
다음과 같은 속성을 사용합니다.
- `mqtt.broker.url`, `mqtt.client.id`, `mqtt.topic`
- `influxdb.url`, `influxdb.token`, `influxdb.org`, `influxdb.bucket`
- `model.api-url`
- `async.core-pool-size`, `async.max-pool-size`

## 프로젝트 구조
- `config`  : MQTT, InfluxDB 등 외부 연동 설정
- `service` : 메시지 구독, InfluxDB 저장, 모델 서비스 호출 로직
- `dto`     : 센서 데이터 전송 객체
- `exception` : 사용자 정의 예외 정의
- `resources` : 기본 설정 파일 및 로그 설정
- `test`    : 단위 테스트 코드

## 라이선스
이 저장소의 코드는 NHN Academy 교육용 예제입니다.
