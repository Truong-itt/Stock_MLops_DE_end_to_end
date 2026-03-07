# ── Stage 1: Build Java project ──────────────────────────────────────
FROM maven:3.8.8-eclipse-temurin-11 AS builder

WORKDIR /build
COPY pom.xml .
# Cache dependencies trước (tận dụng Docker layer cache)
RUN mvn dependency:go-offline -B || true

COPY src ./src
RUN mvn clean package -DskipTests

# ── Stage 2: Flink runtime ──────────────────────────────────────────
FROM flink:1.17.1

USER root

# Copy fat-jar đã build từ stage 1 (tất cả dependencies đã đóng gói trong fat-jar)
COPY --from=builder /build/target/flink-stock-processing-1.0-SNAPSHOT.jar \
     /opt/flink/usrlib/flink-stock-processing-1.0-SNAPSHOT.jar

USER flink