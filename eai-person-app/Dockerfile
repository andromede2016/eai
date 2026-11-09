# Build first: mvn clean install (from /home/badr/git/eai)
# Then: docker build -t eai-person-app:1.0.0-SNAPSHOT eai-person-app
FROM eclipse-temurin:21-jre
RUN useradd --system --uid 1001 eai
WORKDIR /app
COPY target/eai-person-app-*.jar app.jar
USER 1001
EXPOSE 8080
# All configuration comes from environment variables (ConfigMap / Secret in Kubernetes)
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
