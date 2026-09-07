FROM eclipse-temurin:26.0.2_10-jdk AS build
WORKDIR /workspace
COPY . .
RUN chmod +x gradlew
RUN --mount=type=cache,target=/root/.gradle ./gradlew bootJar --no-daemon --console=plain

FROM eclipse-temurin:26.0.2_10-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && groupadd --system app && useradd --system --gid app app
WORKDIR /app
ARG SERVICE
COPY --from=build /workspace/${SERVICE}/build/libs/app.jar app.jar
USER app
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=65 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
