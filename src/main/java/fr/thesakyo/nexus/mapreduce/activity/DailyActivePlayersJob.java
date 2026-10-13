package fr.thesakyo.nexus.mapreduce.activity;

import fr.thesakyo.nexus.mapreduce.common.TelemetryEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.time.ZoneOffset;

/**
 * Produces one row per UTC day and distinct player with a {@code session_start} event.
 *
 * <p>Input is telemetry JSONL. Output rows use {@code date<TAB>player_id} and are intended
 * as the stable input contract for {@link ActivityMetricsJob} and retention processing.</p>
 */
public final class DailyActivePlayersJob {

    /** Separates fields in internal sort keys without colliding with date or ID text. */
    private static final String SEPARATOR = "\u0001";

    /** Prevents instantiation of this job-configuration utility. */
    private DailyActivePlayersJob() {}

    /**
     * Hadoop counters that summarize accepted activity and common input-quality issues.
     */
    public enum QualityCounters {

        /** Telemetry lines presented to the mapper. */
        INPUT_LINES,

        /** Session-start records accepted and emitted as player-day keys. */
        ACCEPTED_SESSION_STARTS,

        /** Valid telemetry records whose event type is not {@code session_start}. */
        IGNORED_EVENT_TYPES,

        /** Session-start records without a usable player identifier. */
        MISSING_PLAYER_ID,

        /** Lines that are not valid JSON objects. */
        MALFORMED_JSON,

        /** Valid JSON values that are not objects. */
        NOT_AN_OBJECT,

        /** Object records with no supported event-type field. */
        MISSING_EVENT_TYPE,

        /** Object records with no supported timestamp field. */
        MISSING_TIMESTAMP,

        /** Records whose timestamp is present but cannot be interpreted. */
        INVALID_TIMESTAMP
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Converts qualifying telemetry events into composite {@code day + player} keys.
     *
     * <p>Only valid {@code session_start} events with a usable player identifier are
     * emitted. Activity dates are derived from event timestamps in UTC.</p>
     */
    public static final class ActivePlayerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        /**
         * Reusable output key to reduce unnecessary object allocations.
         */
        private final Text outputKey = new Text();

        /**
         * Validates and maps one telemetry line to its UTC player-day key.
         *
         * @param offset the byte offset of the input line within its input split
         * @param line the input telemetry record in JSON format
         * @param context the Hadoop mapper context used to emit records and update counters
         * @throws IOException if an input/output error occurs
         * @throws InterruptedException if the mapper is interrupted while writing output
         */
        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

            // Count every telemetry line presented to the mapper.
            context.getCounter(QualityCounters.INPUT_LINES).increment(1);

            // Parse the telemetry record and count any input-quality issue.
            TelemetryEvent event = parse(line, context);

            // Skip records that could not be parsed successfully.
            if(event == null) return;

            /*
             * Keep only session-start events, which define daily player activity.
             */
            if(!"session_start".equals(event.eventType())) {
                context.getCounter(QualityCounters.IGNORED_EVENT_TYPES).increment(1);
                return;
            }

            /*
             * Reject session-start events without a usable player identifier.
             */
            if(event.playerId().isBlank()) {
                context.getCounter(QualityCounters.MISSING_PLAYER_ID).increment(1);
                return;
            }

            // Emit the composite key used to deduplicate the player within the UTC day.
            writePlayerDay(event, context);
        }

        /**
         * Parses telemetry and increments the matching quality counter on failure.
         *
         * @param line the input telemetry record in JSON format
         * @param context the Hadoop mapper context used to update quality counters
         * @return the parsed telemetry event, or {@code null} if the record is invalid
         */
        private TelemetryEvent parse(Text line, Context context) {


            /*
             * Parse the JSON record into a validated telemetry event.
             */
            try { return TelemetryEvent.parse(line.toString()); }
            catch (TelemetryEvent.InvalidRecordException exception) {

                /* Increment the counter corresponding to the parsing failure reason. */
                context.getCounter(QualityCounters.valueOf(exception.reason().name())).increment(1);
                return null;
            }
        }

        /**
         * Emits the composite key used to deduplicate one player within one UTC day.
         *
         * <p>The key combines the UTC activity date and player identifier using
         * {@code SEPARATOR}. The reducer later converts it to the stable tab-separated
         * output format.</p>
         *
         * @param event the validated session-start telemetry event
         * @param context the Hadoop mapper context used to emit the player-day key
         * @throws IOException if an input/output error occurs
         * @throws InterruptedException if the mapper is interrupted while writing output
         */
        private void writePlayerDay(TelemetryEvent event, Context context) throws IOException, InterruptedException {

            // Derive the activity date from the event timestamp in UTC.
            String day = event.timestamp().atOffset(ZoneOffset.UTC).toLocalDate().toString();

            // Build the composite key from the UTC date and player identifier.
            outputKey.set(day + SEPARATOR + event.playerId());

            // Emit the player-day key without an associated value.
            context.write(outputKey, NullWritable.get());

            // Count the accepted session-start event.
            context.getCounter(QualityCounters.ACCEPTED_SESSION_STARTS).increment(1);
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Performs safe local deduplication before keys are shuffled across the network.
     *
     * <p>Repeated player-day keys are reduced locally to limit the number of duplicate
     * keys transferred during the shuffle phase.</p>
     */
    public static final class DistinctPlayerCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {

        /**
         * Emits one key for a local group so fewer duplicate keys cross the shuffle.
         *
         * @param key the composite UTC date and player identifier
         * @param values the values associated with the composite key
         * @param context the Hadoop reducer context used to emit the deduplicated key
         * @throws IOException if an input/output error occurs
         * @throws InterruptedException if the combiner is interrupted while writing output
         */
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

                // Emit the key once, regardless of the number of local duplicates.
                context.write(key, NullWritable.get());
            }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Emits the stable {@code date<TAB>player_id} intermediate format.
     *
     * <p>Hadoop groups identical composite keys before this reducer runs, ensuring
     * that each player appears at most once per UTC activity day.</p>
     */
    public static final class DistinctPlayerReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        /**
         * Reusable output object for the normalized player-day record.
         */
        private final Text output = new Text();

        /**
         * Converts a composite key into the stable tab-separated player-day row.
         *
         * @param compositeKey the composite key containing the UTC date and player ID
         * @param values the values associated with the composite key
         * @param context the Hadoop reducer context used to emit the normalized record
         * @throws IOException if an input/output error occurs
         * @throws InterruptedException if the reducer is interrupted while writing output
         */
        @Override
        protected void reduce(Text compositeKey, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

                // Split the composite key into the date and player identifier.
                String[] fields = compositeKey.toString().split(SEPARATOR, 2);

                /*
                 * Ignore malformed composite keys that do not contain both expected fields.
                 */
                if(fields.length != 2) return;

                // Rebuild the normalized record with tab-separated date and player fields.
                output.set(fields[0] + "\t" + fields[1]);

                // Emit the distinct player-day record without an associated value.
                context.write(output, NullWritable.get());
            }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Creates the Hadoop job without submitting it, allowing reuse by other launchers.
     *
     * <p>The job parses telemetry JSONL, filters qualifying session-start events,
     * and deduplicates player activity by UTC day.</p>
     *
     * @param configuration the Hadoop configuration to use
     * @param input the telemetry JSONL input path
     * @param output the destination for distinct {@code date<TAB>player_id} rows
     * @return a configured job ready for submission
     * @throws IOException if Hadoop cannot configure an input or output path
     */
    public static Job configure(Configuration configuration, Path input, Path output) throws IOException {

        // Create the job with a descriptive name.
        Job job = Job.getInstance(configuration, "nexus-daily-active-players");

        // Specify the class containing the job implementation.
        job.setJarByClass(DailyActivePlayersJob.class);

        /*
         * Configure text input and map telemetry events to composite player-day keys.
         */
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ActivePlayerMapper.class);

        /*
         * Deduplicate player-day keys locally and then globally.
         */
        job.setCombinerClass(DistinctPlayerCombiner.class);
        job.setReducerClass(DistinctPlayerReducer.class);

        /* Define the intermediate key and value types emitted by the mapper. */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        /* Define the final key and value types written by the reducer. */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        /* Write the results as text files. */
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Register the input and output paths for the job. */
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // Return the configured job without executing it.
        return job;
    }

    /**
     * Runs the daily-active-player job from the command line.
     *
     * <p>Expected arguments: {@code <telemetry-jsonl-input> <distinct-day-player-output>}.</p>
     *
     * @param args exactly two Hadoop paths: telemetry input followed by output
     * @throws Exception if job configuration or execution fails
     */
    public static void main(String[] args) throws Exception {

        /*
         * Require the telemetry input path and the distinct player-day output path.
         */
        if(args.length != 2) {
            System.err.println(
                "Usage: DailyActivePlayersJob <telemetry-jsonl-input> "
                    + "<distinct-day-player-output>");
            System.exit(2);
        }

        // Configure the job using the supplied input and output paths.
        Job job = configure(new Configuration(), new Path(args[0]), new Path(args[1]));

        // Execute the job and return its success or failure status.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
