# Uma imagem para os cinco serviços; o profile escolhe qual sobe (SPRING_PROFILES_ACTIVE).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q --no-transfer-progress dependency:go-offline
COPY src ./src
RUN mvn -B -q --no-transfer-progress -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/target/mktplace-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
