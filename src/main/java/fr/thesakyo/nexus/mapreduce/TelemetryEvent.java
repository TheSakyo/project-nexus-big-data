package fr.thesakyo.nexus.mapreduce;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

/** Normalized view of the telemetry schemas announced in the official catalog. */
public record TelemetryEvent(
        String eventId,
        String eventType,
        Instant timestamp,
        String playerId,
        String sessionId,
        String gameVersion,
        String platform,
        String region,
        JsonNode payload) {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<DateTimeFormatter> LOCAL_FORMATS = List.of(
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss[.SSS]")
    );
    private static final List<DateTimeFormatter> DATE_FORMATS = List.of(
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("dd/MM/yyyy")
    );

    public static TelemetryEvent parse(String line) throws InvalidRecordException {
        final JsonNode root;
        try {
            root = MAPPER.readTree(line);
        } catch (JsonProcessingException exception) {
            throw new InvalidRecordException(InvalidReason.MALFORMED_JSON, exception);
        }
        if (root == null || !root.isObject()) {
            throw new InvalidRecordException(InvalidReason.NOT_AN_OBJECT);
        }

        String eventType = firstText(root, "event_type", "evt");
        if (eventType.isBlank()) {
            throw new InvalidRecordException(InvalidReason.MISSING_EVENT_TYPE);
        }
        JsonNode rawTimestamp = firstPresent(root, "timestamp", "ts");
        if (rawTimestamp == null || rawTimestamp.isNull()) {
            throw new InvalidRecordException(InvalidReason.MISSING_TIMESTAMP);
        }
        Instant timestamp;
        try {
            timestamp = parseTimestamp(rawTimestamp);
        } catch (DateTimeParseException | ArithmeticException exception) {
            throw new InvalidRecordException(InvalidReason.INVALID_TIMESTAMP, exception);
        }

        JsonNode payload = root.path("payload");
        if (!payload.isObject()) {
            payload = JsonNodeFactory.instance.objectNode();
        }
        String platform = firstText(root, "platform");
        if (platform.isBlank()) {
            platform = firstText(payload, "platform");
        }

        return new TelemetryEvent(
                firstText(root, "event_id", "id"),
                eventType,
                timestamp,
                firstText(root, "player_id", "pid"),
                firstText(root, "session_id", "session"),
                firstText(root, "game_version", "ver"),
                platform,
                firstText(root, "region"),
                payload);
    }

    /** Parses catalog-listed timestamp variants; timezone-less values are interpreted as UTC. */
    public static Instant parseTimestamp(JsonNode node) {
        if (node.isObject() && node.has("$date")) {
            node = node.get("$date");
        }
        if (node.isObject() && node.has("$numberLong")) {
            node = JsonNodeFactory.instance.numberNode(Long.parseLong(node.path("$numberLong").asText()));
        }
        if (node.isNumber()) {
            long value = node.longValue();
            if (value >= 100_000_000_000L || value <= -100_000_000_000L) {
                return Instant.ofEpochMilli(value);
            }
            return Instant.ofEpochSecond(value);
        }

        String value = node.asText().trim();
        if (value.isEmpty()) {
            throw new DateTimeParseException("Timestamp is blank", value, 0);
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException ignored) {
            // Try the other documented ISO and local representations below.
        }
        try {
            return OffsetDateTime.parse(value, DateTimeFormatter.ISO_DATE_TIME).toInstant();
        } catch (DateTimeParseException ignored) {
            // Continue with timezone-less formats, interpreted as UTC.
        }
        for (DateTimeFormatter formatter : LOCAL_FORMATS) {
            try {
                return LocalDateTime.parse(value, formatter).toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException ignored) {
                // Try the next documented representation.
            }
        }
        for (DateTimeFormatter formatter : DATE_FORMATS) {
            try {
                return LocalDate.parse(value, formatter).atStartOfDay().toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException ignored) {
                // Try the next documented representation.
            }
        }
        throw new DateTimeParseException("Unsupported timestamp format", value, 0);
    }

    public static String text(JsonNode parent, String field) {
        JsonNode value = parent == null ? null : parent.get(field);
        if (value == null || value.isNull()) {
            return "";
        }
        if (value.isObject() && value.has("$oid")) {
            return value.path("$oid").asText("").trim();
        }
        return value.asText("").trim();
    }

    private static String firstText(JsonNode node, String... names) {
        for (String name : names) {
            String value = text(node, name);
            if (!value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    private static JsonNode firstPresent(JsonNode node, String... names) {
        for (String name : names) {
            JsonNode value = node.get(name);
            if (value != null && !value.isNull()) {
                return value;
            }
        }
        return null;
    }

    public enum InvalidReason {
        MALFORMED_JSON,
        NOT_AN_OBJECT,
        MISSING_EVENT_TYPE,
        MISSING_TIMESTAMP,
        INVALID_TIMESTAMP
    }

    public static final class InvalidRecordException extends Exception {
        private final InvalidReason reason;

        public InvalidRecordException(InvalidReason reason) {
            super(reason.name());
            this.reason = reason;
        }

        public InvalidRecordException(InvalidReason reason, Throwable cause) {
            super(reason.name(), cause);
            this.reason = reason;
        }

        public InvalidReason reason() {
            return reason;
        }
    }
}
