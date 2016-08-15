import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.ConsoleAppender

appender("STDOUT", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%d{HH:mm:ss.SSS} [%thread] %highlight(%-5level) %logger{40}: %msg%n"
    }
}

root(INFO, ["STDOUT"])

logger("org.jnaalisv.sqlmapper", INFO)
logger("org.springframework", INFO)

