package fr.thesakyo.nexus.mapreduce.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Instant;

/**
 * Minimal, privacy-aware projection of a MongoDB player document.
 *
 * <p>Only fields required by the prepared analyses are retained. Personal and
 * payment-related information is deliberately excluded from this representation.</p>
 *
 * @param playerId the stable source identifier used only during the reduce-side join
 * @param createdAt the account creation instant, or {@code null} when absent or invalid
 * @param platform the hardware platform from {@code profile.platform}
 * @param gpuVendor the hardware vendor from {@code profile.gpu_vendor}
 */
public record PlayerDocument(String playerId, Instant createdAt, String platform, String gpuVendor) {

    /**
     * Shared JSON parser; Jackson's configured mapper is safe for concurrent reads.
     */
    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Parses one JSONL player snapshot without retaining email, username, or payment data.
     *
     * <p>Extracts only the player identifier, account creation timestamp, hardware
     * platform, and GPU vendor required by downstream analyses.</p>
     *
     * @param line one JSON object from the players snapshot
     * @return the projected player record, with missing fields represented by empty
     *         values or {@code null} for the creation timestamp
     * @throws IOException if the line cannot be parsed as valid JSON
     */
    public static PlayerDocument parse(String line) throws IOException {

        // Parse the input line into a JSON tree.
        JsonNode root = JSON.readTree(line);

        // Retrieve the nested profile object when the root is available.
        JsonNode profile = root == null ? null : root.path("profile");

        /*
         * Build the privacy-aware projection using only the required fields.
         */
        return new PlayerDocument(
            TelemetryEvent.text(root, "_id"),
            readCreatedAt(root),
            TelemetryEvent.text(profile, "platform"),
            TelemetryEvent.text(profile, "gpu_vendor")
        );
    }

    /**
     * Reads and normalizes {@code created_at}; unsupported values are treated as unavailable.
     *
     * <p>Missing, null, or unparseable timestamps do not prevent the player document
     * from being processed. Instead, the creation instant is represented by {@code null}.</p>
     *
     * @param root the parsed JSON object representing the player document
     * @return the normalized account creation instant, or {@code null} if unavailable
     */
    private static Instant readCreatedAt(JsonNode root) {

        // Retrieve the account creation timestamp when the root object exists.
        JsonNode value = root == null ? null : root.get("created_at");

        // Treat missing and explicit JSON null values as unavailable.
        if(value == null || value.isNull()) return null;

        /*
         * Parse the timestamp; unsupported or malformed values are treated as unavailable.
         */
        try { return TelemetryEvent.parseTimestamp(value); }
        catch(RuntimeException exception) { return null; }
    }
}
