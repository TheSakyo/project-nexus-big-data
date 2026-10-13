package fr.thesakyo.nexus.mapreduce.technical;

import fr.thesakyo.nexus.mapreduce.common.PlayerDocument;
import fr.thesakyo.nexus.mapreduce.common.PlayerJoinKey;
import fr.thesakyo.nexus.mapreduce.common.TelemetryEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/**
 * Joins technical events to player profiles using a streaming reduce-side join.
 *
 * <p>One intermediate row is written per player/day/version/platform/GPU combination,
 * containing session starts, crashes, and disconnects. Personal profile fields are discarded.</p>
 */
public final class TechnicalIncidentsByProfileJob {

    /** Prevents instantiation of this command-line job class. */
    private TechnicalIncidentsByProfileJob() {}

    /**
     * Counters used to assess how much technical telemetry can be joined reliably.
     */
    public enum QualityCounters {

        /** Player snapshot lines read by the profile mapper. */
        PLAYER_LINES,

        /** Identified player profile records emitted for the join. */
        PLAYER_RECORDS,

        /** Player records without a source identifier. */
        PLAYER_RECORDS_WITHOUT_ID,

        /** Player lines that cannot be parsed as JSON documents. */
        MALFORMED_PLAYER_JSON,

        /** Telemetry lines read by the event mapper. */
        TELEMETRY_LINES,

        /** Supported session or incident events selected for the analysis. */
        SELECTED_TELEMETRY_EVENTS,

        /** Selected events without a usable player identifier. */
        TELEMETRY_WITHOUT_PLAYER_ID,

        /** Telemetry lines rejected by the shared parser. */
        TELEMETRY_INVALID,

        /** Player groups with events but no matching profile record. */
        PLAYERS_WITHOUT_PROFILE,

        /** Repeated player profiles that make the join ambiguous. */
        DUPLICATE_PLAYER_RECORDS
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Emits only profile fields needed for technical grouping; personal fields are discarded.
     */
    public static final class PlayerMapper extends Mapper<LongWritable, Text, Text, Text> {

        /** Reusable Hadoop output key for the composite player identifier. */
        private final Text outputKey = new Text();

        /** Reusable Hadoop output value containing the hardware profile dimensions. */
        private final Text outputValue = new Text();

        /**
         * Projects one player document to the non-personal profile dimensions used downstream.
         *
         * @param offset the input line offset
         * @param line the raw JSONL player document
         * @param context the mapper context used to emit records and quality counters
         * @throws IOException if writing the mapped record fails
         * @throws InterruptedException if the mapper is interrupted
         */
        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

            // Count every player snapshot line received by the mapper.
            context.getCounter(QualityCounters.PLAYER_LINES).increment(1);

            /*
             * Parse the player document into the minimal projection used by the join.
             */
            final PlayerDocument player;
            try { player = PlayerDocument.parse(line.toString()); }
            catch(IOException | RuntimeException exception) {
                context.getCounter(QualityCounters.MALFORMED_PLAYER_JSON).increment(1);
                return;
            }

            /*
             * Reject player records without a usable source identifier.
             */
            if(player.playerId().isBlank()) {
                context.getCounter(QualityCounters.PLAYER_RECORDS_WITHOUT_ID).increment(1);
                return;
            }

            // Build a profile-side composite key for secondary sorting.
            outputKey.set(PlayerJoinKey.composite(player.playerId(), PlayerJoinKey.PROFILE_TAG));

            /*
             * Emit only the hardware dimensions required by downstream aggregation.
             * Personal profile fields are deliberately excluded from the join value.
             */
            outputValue.set("P\t" + clean(player.platform())
                + "\t" + clean(player.gpuVendor()));

            // Send the projected player profile to the reducer.
            context.write(outputKey, outputValue);

            // Count player profiles successfully emitted for the join.
            context.getCounter(QualityCounters.PLAYER_RECORDS).increment(1);
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Emits session starts and technical incidents under the matching player key.
     */
    public static final class IncidentMapper extends Mapper<LongWritable, Text, Text, Text> {

        /** Reusable Hadoop output key for the composite player identifier. */
        private final Text outputKey = new Text();

        /** Reusable Hadoop output value containing the event details. */
        private final Text outputValue = new Text();

        /**
         * Selects session and incident events and emits them under the event-side join tag.
         *
         * @param offset the input line offset
         * @param line the raw telemetry JSONL record
         * @param context the mapper context used to emit records and quality counters
         * @throws IOException if writing the mapped record fails
         * @throws InterruptedException if the mapper is interrupted
         */
        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

            // Count every telemetry line received by the mapper.
            context.getCounter(QualityCounters.TELEMETRY_LINES).increment(1);

            /*
             * Parse the event using the shared telemetry parser.
             */
            final TelemetryEvent event;
            try { event = TelemetryEvent.parse(line.toString()); }
            catch(TelemetryEvent.InvalidRecordException exception) {
                context.getCounter(QualityCounters.TELEMETRY_INVALID).increment(1);
                return;
            }

            /*
             * Keep only session starts, crashes, and disconnects.
             * Other valid event types are not required for this technical analysis.
             */
            if (!("session_start".equals(event.eventType())
                || "crash".equals(event.eventType())
                || "disconnect".equals(event.eventType()))) {
                return;
            }

            // Count events selected for the technical analysis.
            context.getCounter(QualityCounters.SELECTED_TELEMETRY_EVENTS).increment(1);

            /*
             * Reject selected events without a usable player identifier.
             */
            if(event.playerId().isBlank()) {
                context.getCounter(QualityCounters.TELEMETRY_WITHOUT_PLAYER_ID).increment(1);
                return;
            }

            // Convert the event timestamp to a UTC calendar date.
            String day = event.timestamp().atOffset(ZoneOffset.UTC).toLocalDate().toString();

            // Build an event-side composite key for the player.
            outputKey.set(PlayerJoinKey.composite(event.playerId(), PlayerJoinKey.EVENT_TAG));

            /*
             * Encode the event date, type, and normalized game version.
             */
            outputValue.set("E\t" + day + "\t" + clean(event.eventType()) + "\t"
                + clean(nonEmpty(event.gameVersion(), "unknown")));

            // Send the selected event to the reducer.
            context.write(outputKey, outputValue);
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Aggregates each player's events by day, version, and hardware profile.
     */
    public static final class JoinReducer extends Reducer<Text, Text, Text, Text> {

        /** Reusable Hadoop output key containing the aggregation dimensions. */
        private final Text outputKey = new Text();

        /** Reusable Hadoop output value containing the three event counts. */
        private final Text outputValue = new Text();

        /**
         * Reads the profile-first player group and aggregates its events by output dimension.
         *
         * @param compositeKey the composite key identifying the player
         * @param values the secondary-sorted profile and event values for that player
         * @param context the reducer context used to emit results and quality counters
         * @throws IOException if writing an aggregated record fails
         * @throws InterruptedException if the reducer is interrupted
         */
        @Override
        protected void reduce(Text compositeKey, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

                // Initialize the state used to collect the player's profile and event counts.
                JoinState state = new JoinState();

                // Process the profile and event values associated with this player.
                for(Text value : values) state.accept(value.toString(), context);

                /*
                 * Count player groups that contain events but no matching profile.
                 */
                if(!state.profileSeen) {
                    context.getCounter(QualityCounters.PLAYERS_WITHOUT_PROFILE).increment(1);
                    return;
                }

                // Skip ambiguous joins when multiple profiles exist for the same player.
                if(state.duplicateProfile) return;

                // Emit the aggregated counts for each day and technical dimension.
                writePlayerCounts(state, context);
            }

        /**
         * Emits each day/version/platform/GPU count tuple accumulated for this player.
         *
         * @param state the collected profile and event counts
         * @param context the reducer context used to write aggregated records
         * @throws IOException if writing an aggregated record fails
         * @throws InterruptedException if the reducer is interrupted
         */
        private void writePlayerCounts(JoinState state, Context context) throws IOException, InterruptedException {

            // Write one output row for each dimension observed for the current player.
            for (Map.Entry<String, long[]> entry : state.countsByDimension.entrySet()) {

                // Retrieve the session, crash, and disconnect totals for this dimension.
                long[] counts = entry.getValue();

                // Set the output key to the day, version, platform, and GPU vendor.
                outputKey.set(entry.getKey());

                // Serialize the three event counts as tab-separated fields.
                outputValue.set(counts[0] + "\t" + counts[1] + "\t" + counts[2]);

                // Emit the player's aggregated counts for this dimension.
                context.write(outputKey, outputValue);
            }
        }

        /**
         * Holds one player's dimensions and aggregated technical-event counts.
         */
        private final class JoinState {

            /** Player platform, or {@code unknown} when unavailable. */
            private String platform = "unknown";

            /** Player GPU vendor, or {@code unknown} when unavailable. */
            private String gpuVendor = "unknown";

            /** Indicates whether a player profile has been processed. */
            private boolean profileSeen;

            /** Indicates that multiple profiles make the join ambiguous. */
            private boolean duplicateProfile;

            /** Aggregated session, crash, and disconnect counts keyed by technical dimensions. */
            private final Map<String, long[]> countsByDimension = new HashMap<>();

            /**
             * Dispatches a tagged shuffle value to the matching profile or event handler.
             *
             * @param rawValue the tagged, tab-separated mapper value
             * @param context the reducer context used to increment quality counters
             */
            private void accept(String rawValue, Context context) {

                // Split the value while preserving empty trailing fields.
                String[] fields = rawValue.split("\t", -1);

                // Ignore values that do not contain a record type.
                if(fields.length == 0) return;

                /*
                 * Route the value to the handler matching its record type.
                 */
                if("P".equals(fields[0])) acceptProfile(fields, context);
                else if ("E".equals(fields[0])) acceptEvent(fields);
            }

            /**
             * Captures the first profile and marks multiple profiles as an ambiguous join.
             *
             * @param fields the tagged profile fields
             * @param context the reducer context used to increment quality counters
             */
            private void acceptProfile(String[] fields, Context context) {

                /*
                 * Detect duplicate profiles for the same player identifier.
                 */
                if(profileSeen) {
                    context.getCounter(QualityCounters.DUPLICATE_PLAYER_RECORDS).increment(1);
                    duplicateProfile = true;
                    return;
                }

                // Mark the first profile as processed.
                profileSeen = true;

                // Read the platform and GPU vendor, using unknown when values are missing.
                platform = fieldOrUnknown(fields, 1);
                gpuVendor = fieldOrUnknown(fields, 2);
            }

            /**
             * Adds one selected event to its date, version, and profile count bucket.
             *
             * @param fields the tagged event fields
             */
            private void acceptEvent(String[] fields) {

                // Ignore event values that do not contain all required fields.
                if(fields.length < 4) return;

                /*
                 * Build the aggregation dimension using the event and player profile fields.
                 */
                String dimension = String.join("\t", fields[1],
                    nonEmpty(fields[3], "unknown"),
                    platform, gpuVendor);

                // Retrieve the existing counters or initialize a new set of three counters.
                long[] counts = countsByDimension.computeIfAbsent(dimension, ignored -> new long[3]);

                /*
                 * Increment the counter corresponding to the event type.
                 */
                switch(fields[2]) {
                    case "session_start" -> counts[0]++;
                    case "crash" -> counts[1]++;
                    case "disconnect" -> counts[2]++;
                    default -> {/* Ignore event types outside the supported technical categories. */}
                }
            }
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Replaces TSV-breaking control characters and trims a source dimension value.
     *
     * @param value the source dimension value
     * @return the cleaned value, or an empty string if the input is {@code null}
     */
    private static String clean(String value) {

        /*
         * Replace control characters that would otherwise corrupt tab-separated output.
         */
        return value == null ? "" : value.replace('\t', ' ')
            .replace('\n', ' ')
            .replace('\r', ' ')
            .trim();
    }

    /**
     * Returns a cleaned value or the supplied fallback when the value is blank.
     *
     * @param value the source value
     * @param fallback the value to use when the source is blank
     * @return the cleaned source value, or the fallback
     */
    private static String nonEmpty(String value, String fallback) {

        // Normalize the source value before checking whether it is blank.
        String cleaned = clean(value);

        // Use the fallback when the cleaned value contains no text.
        return cleaned.isBlank() ? fallback : cleaned;
    }

    /**
     * Reads an optional tagged-field value and applies the standard unknown fallback.
     *
     * @param fields the tagged record fields
     * @param index the index of the field to read
     * @return the cleaned field value, or {@code unknown} when unavailable
     */
    private static String fieldOrUnknown(String[] fields, int index) {

        // Read the field when the requested index exists; otherwise use the fallback.
        return fields.length > index ? nonEmpty(fields[index], "unknown") : "unknown";
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Command-line entry point.
     *
     * <p>Usage: {@code <players-jsonl> <telemetry-jsonl> <joined-output>}</p>
     *
     * @param args player JSONL path, telemetry JSONL path, and new joined-output path
     * @throws Exception if Hadoop job setup or execution fails
     */
    public static void main(String[] args) throws Exception {

        // Validate the number of command-line arguments.
        if(args.length != 3) {
            System.err.println(
                "Usage: TechnicalIncidentsByProfileJob <players-jsonl> <telemetry-jsonl> <joined-output>"
            );
            System.exit(2);
        }

        // Create the Hadoop configuration used by the MapReduce job.
        Configuration configuration = new Configuration();

        // Initialize the job with its descriptive name.
        Job job = Job.getInstance(configuration, "nexus-technical-incidents-by-player-profile");

        /* Configure the job class and intermediate key-value types. */
        job.setJarByClass(TechnicalIncidentsByProfileJob.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        /* Configure player-based partitioning and grouping for the reduce-side join. */
        job.setPartitionerClass(PlayerJoinKey.PartitionerByPlayer.class);
        job.setGroupingComparatorClass(PlayerJoinKey.GroupingComparator.class);

        /* Configure the reducer and final output types. */
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /*
         * Register both input sources with their respective mappers.
         * Player profiles and selected telemetry events are joined by player identifier.
         */
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PlayerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, IncidentMapper.class);

        // Configure the destination for the joined technical event counts.
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Return a process exit code based on the job execution result.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
