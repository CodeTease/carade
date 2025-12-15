# Stage 1: Build
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /src
COPY core/ core/
RUN javac core/*.java

# Stage 2: Runtime
FROM eclipse-temurin:21-jre-alpine

# Create data directory
WORKDIR /data

# Copy compiled classes to a separate code directory
COPY --from=builder /src/core /app/core

# Expose default port
EXPOSE 63790

# Persistence volume
VOLUME /data

# Run from /data so that carade.dump/aof/conf are stored/read from the volume
# Classpath points to the code directory
CMD ["java", "-cp", "/app/core", "Carade"]
