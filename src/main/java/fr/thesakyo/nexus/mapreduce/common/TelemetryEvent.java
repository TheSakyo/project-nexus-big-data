package fr.thesakyo.nexus.mapreduce.common;

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

/**
 * Normalized view of a telemetry event across the catalog's v1 and v2 field names.
 *
 * <p>Timestamp values are converted to absolute instants so that jobs can consistently
 * derive UTC dates, regardless of the timestamp representation used by the source.</p>
 *
 * @param eventId the source event identifier, retained for future quality analysis
 * @param eventType the canonical event type
 * @param timestamp the event timestamp as an absolute instant
 * @param playerId the canonical player identifier
 * @param sessionId the canonical session identifier
 * @param gameVersion the canonical game version
 * @param platform the event platform, when present at the root or in the payload
 * @param region the event region
 * @param payload optional event-specific fields
 */
public record TelemetryEvent(String eventId, String eventType, Instant timestamp, String playerId, String sessionId,
                             String gameVersion, String platform, String region, JsonNode payload) {

    /** Shared JSON parser reused across mapper and reducer calls. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Local date-time formats supported by the published data catalog. */
    private static final List<DateTimeFormatter> LOCAL_FORMATS = List.of(
        DateTimeFormatter.ISO_LOCAL_DATE_TIME,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]"),
        DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss[.SSS]")
    );

    /** Date-only formats interpreted as midnight UTC. */
    private static final List<DateTimeFormatter> DATE_FORMATS = List.of(
        DateTimeFormatter.ISO_LOCAL_DATE,
        DateTimeFormatter.ofPattern("dd/MM/yyyy")
    );

    /**
     * Parses one telemetry JSON line and maps schema v1/v2 aliases to canonical fields.
     *
     * <p>The method validates the JSON structure, event type, and timestamp before
     * creating a normalized event. Missing optional fields are represented by empty
     * strings, while a missing or invalid required field causes a categorized exception.</p>
     *
     * @param line the raw JSONL line
     * @return the normalized telemetry event
     * @throws InvalidRecordException if the JSON structure, event type, or timestamp is invalid
     */
    public static TelemetryEvent parse(String line) throws InvalidRecordException {

        /*
         * Parse the input line and preserve a categorized error when the JSON is malformed.
         */
        final JsonNode root;
        try { root = MAPPER.readTree(line); }
        catch(JsonProcessingException exception) {
            throw new InvalidRecordException(InvalidReason.MALFORMED_JSON, exception);
        }

        /* Validate the JSON structure before attempting to read event fields. */
        if(root == null || !root.isObject()) throw new InvalidRecordException(InvalidReason.NOT_AN_OBJECT);

        /*
         * Resolve the event type from the supported schema aliases.
         */
        String eventType = firstText(root, "event_type", "evt");
        if(eventType.isBlank()) throw new InvalidRecordException(InvalidReason.MISSING_EVENT_TYPE);

        /*
         * Retrieve the first available timestamp field.
         */
        JsonNode rawTimestamp = firstPresent(root, "timestamp", "ts");
        if(rawTimestamp == null || rawTimestamp.isNull()) throw new InvalidRecordException(InvalidReason.MISSING_TIMESTAMP);

        /*
         * Normalize the timestamp and classify parsing failures for Hadoop quality counters.
         */
        Instant timestamp;
        try { timestamp = parseTimestamp(rawTimestamp); }
        catch(DateTimeParseException | ArithmeticException | NumberFormatException exception) {
            throw new InvalidRecordException(InvalidReason.INVALID_TIMESTAMP, exception);
        }

        /* Ensure the payload is always represented by a JSON object.
         * This lets downstream jobs access payload fields without handling other JSON types.
         */
        JsonNode payload = root.path("payload");
        if(!payload.isObject()) payload = JsonNodeFactory.instance.objectNode();

        /*
         * Prefer the root-level platform and fall back to the payload when it is absent.
         */
        String platform = firstText(root, "platform");
        if(platform.isBlank()) platform = firstText(payload, "platform");

        /*
         * Build the normalized event using the canonical field names.
         */
        return new TelemetryEvent(
            firstText(root, "event_id", "id"),
            eventType,
            timestamp,
            firstText(root, "player_id", "pid"),
            firstText(root, "session_id", "session"),
            firstText(root, "game_version", "ver"),
            platform,
            firstText(root, "region"),
            payload
        );
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Parses catalog-listed ISO, local, date-only, and Unix timestamp representations.
     *
     * <p>Values without an explicit timezone are interpreted as UTC. Numeric values
     * below the millisecond epoch threshold are interpreted as epoch seconds. MongoDB
     * Extended JSON {@code $date} and {@code $numberLong} wrappers are also supported.</p>
     *
     * @param node the timestamp value from a JSON document
     * @return the timestamp as an absolute instant
     * @throws DateTimeParseException if the value does not match a supported format
     */
    public static Instant parseTimestamp(JsonNode node) {

        // Unwrap MongoDB Extended JSON date values.
        if(node.isObject() && node.has("$date")) node = node.get("$date");

        /*
         * Convert MongoDB Extended JSON numeric timestamps.
         */
        if(node.isObject() && node.has("$numberLong"))
            return parseEpoch(Long.parseLong(node.path("$numberLong").asText()));

        /*
         * Interpret numeric timestamps using the catalog's seconds-or-milliseconds rule.
         */
        if(node.isNumber()) return parseEpoch(node.longValue());

        // Parse textual timestamps using the supported formats.
        return parseTextTimestamp(node.asText().trim());
    }

    /**
     * Converts epoch seconds or milliseconds to an instant using the catalog's magnitude rule.
     *
     * @param value the numeric Unix timestamp
     * @return the corresponding absolute instant
     */
    private static Instant parseEpoch(long value) {

        // Treat values with a millisecond-scale magnitude as epoch milliseconds.
        if(value >= 100_000_000_000L || value <= -100_000_000_000L) return Instant.ofEpochMilli(value);

        // Treat smaller values as epoch seconds.
        return Instant.ofEpochSecond(value);
    }

    /**
     * Tries supported textual timestamp forms in a stable, documented order.
     *
     * @param value the trimmed timestamp string
     * @return the parsed absolute instant
     * @throws DateTimeParseException if none of the supported formats match
     */
    private static Instant parseTextTimestamp(String value) {

        // Reject empty strings before attempting format parsing.
        if(value.isEmpty()) throw new DateTimeParseException("Timestamp is blank", value, 0);

        // Try the standard ISO instant format, which includes timezone information.
        try { return Instant.parse(value); }

        // Try the other documented ISO and local representations below.
        catch(DateTimeParseException ignored) {}

        // Try an ISO date-time containing an explicit offset or timezone.
        try { return OffsetDateTime.parse(value, DateTimeFormatter.ISO_DATE_TIME).toInstant(); }

        // Continue with timezone-less formats, interpreted as UTC.
        catch(DateTimeParseException ignored) {}

        /*
         * Try supported date-time strings without an explicit timezone.
         */
        Instant localDateTime = parseWithLocalDateTimeFormats(value);
        if(localDateTime != null) return localDateTime;

        /*
         * Try supported date-only strings.
         */
        Instant date = parseWithDateFormats(value);
        if(date != null) return date;

        // Reject values that do not match any supported representation.
        throw new DateTimeParseException("Unsupported timestamp format", value, 0);
    }

    /**
     * Parses timezone-free date-time strings and assigns UTC to them.
     *
     * @param value the timestamp string to parse
     * @return the corresponding instant, or {@code null} if no format matches
     */
    private static Instant parseWithLocalDateTimeFormats(String value) {

        /*
         * Attempt each documented local date-time format in order.
         */
        for(DateTimeFormatter formatter : LOCAL_FORMATS) {
            try { return LocalDateTime.parse(value, formatter).toInstant(ZoneOffset.UTC); }
            catch(DateTimeParseException ignored) {}
        }

        // Signal that none of the local date-time formats matched.
        return null;
    }

    /**
     * Parses date-only strings as midnight UTC; returns {@code null} if none match.
     *
     * @param value the date string to parse
     * @return the start-of-day instant in UTC, or {@code null} if no format matches
     */
    private static Instant parseWithDateFormats(String value) {

        /*
         * Attempt each documented date-only format in order.
         */
        for(DateTimeFormatter formatter : DATE_FORMATS) {
            try { return LocalDate.parse(value, formatter).atStartOfDay().toInstant(ZoneOffset.UTC); }
            catch(DateTimeParseException ignored) {}
        }

        // Signal that none of the date-only formats matched.
        return null;
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Reads a scalar JSON field as trimmed text and unwraps MongoDB {@code $oid} values.
     *
     * @param parent the JSON object containing the field
     * @param field the field name to read
     * @return the trimmed field value, or an empty string when unavailable
     */
    public static String text(JsonNode parent, String field) {

        // Retrieve the field only when the parent object is available.
        JsonNode value = parent == null ? null : parent.get(field);

        // Represent missing and explicit JSON null values as empty strings.
        if(value == null || value.isNull()) return "";

        /*
         * Unwrap MongoDB Extended JSON object identifiers.
         */
        if(value.isObject() && value.has("$oid")) {
            return value.path("$oid").asText("").trim();
        }

        // Return the scalar value as trimmed text.
        return value.asText("").trim();
    }

    /**
     * Returns the first nonblank scalar value among the supplied schema aliases.
     *
     * @param node the JSON object containing the candidate fields
     * @param names the field names to try, in priority order
     * @return the first nonblank value, or an empty string if none is available
     */
    private static String firstText(JsonNode node, String... names) {

        /*
         * Check each alias in the specified priority order.
         */
        for(String name : names) {
            String value = text(node, name);
            if(!value.isBlank()) return value;
        }

        // No alias contained a usable value.
        return "";
    }

    /**
     * Returns the first present, non-null JSON value among the supplied field aliases.
     *
     * @param node the JSON object containing the candidate fields
     * @param names the field names to try, in priority order
     * @return the first present, non-null value, or {@code null} if none is available
     */
    private static JsonNode firstPresent(JsonNode node, String... names) {

        /*
         * Check each alias in the specified priority order.
         */
        for(String name : names) {
            JsonNode value = node.get(name);
            if(value != null && !value.isNull()) return value;
        }

        // No alias contained a usable value.
        return null;
    }

    /**
     * Stable categories used by callers to count malformed telemetry records.
     */
    public enum InvalidReason {

        /** The line could not be decoded as JSON. */
        MALFORMED_JSON,

        /** The JSON value was not an object. */
        NOT_AN_OBJECT,

        /** No supported event type alias was present. */
        MISSING_EVENT_TYPE,

        /** No supported timestamp alias was present. */
        MISSING_TIMESTAMP,

        /** A timestamp was present but did not match a supported representation. */
        INVALID_TIMESTAMP
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Checked parse failure that carries a counter-friendly reason and optional cause.
     */
    public static final class InvalidRecordException extends Exception {

        /** The stable category associated with this parse failure. */
        private final InvalidReason reason;

        /**
         * Creates a categorized telemetry parse failure without a lower-level cause.
         *
         * @param reason the stable category used by Hadoop quality counters
         */
        public InvalidRecordException(InvalidReason reason) {
            super(reason.name());
            this.reason = reason;
        }

        /**
         * Creates a categorized telemetry parse failure and preserves its lower-level cause.
         *
         * @param reason the stable category used by Hadoop quality counters
         * @param cause the parser exception that explains the failure
         */
        public InvalidRecordException(InvalidReason reason, Throwable cause) {
            super(reason.name(), cause);
            this.reason = reason;
        }

        /**
         * Returns the stable category associated with this parse failure.
         *
         * @return the parse-failure category used by Hadoop quality counters
         */
        public InvalidReason reason() { return reason; }
    }
}
