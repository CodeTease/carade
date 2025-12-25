# Stage 1: Build
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Run
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/carade-*.jar app.jar
EXPOSE 63790
VOLUME /data
WORKDIR /data
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
