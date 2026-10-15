package fr.thesakyo.nexus.mapreduce.activity;

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
import java.time.LocalDate;
import java.time.format.DateTimeParseException;

/**
 * Builds daily active users and rolling seven-UTC-day active users from
 * {@link DailyActivePlayersJob} output.
 *
 * <p>The first stage deduplicates player membership per metric window. The second stage
 * counts those memberships, so repeated activity does not inflate a metric.
 */
public final class ActivityMetricsJob {

    /** Separates fields in internal sort keys without colliding with date or ID text. */
    private static final String SEPARATOR = "\u0001";

    /** Prevents instantiation of this job-configuration utility. */
    private ActivityMetricsJob() {}

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Expands each daily active player into their DAU day and applicable WAU windows.
     *
     * <p>For each distinct player-day record, this mapper emits a DAU membership key
     * and up to seven WAU membership keys, provided that the corresponding windows
     * fall within the fully observed period.</p>
     *
     * <p>Invalid records and activity dates are counted using Hadoop counters and
     * skipped without interrupting the job.</p>
     */
    public static final class ExpandWindowsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        /** Reusable output key to reduce unnecessary object allocations. */
        private final Text outputKey = new Text();

        /** First observation date for which seven-day activity windows are complete. */
        private LocalDate observationStart;

        /**
         * Loads and validates the first date for which all seven-day windows are complete.
         *
         * <p>The date is read from the Hadoop configuration property
         * {@code nexus.activity.start-date}.</p>
         *
         * @param context the Hadoop mapper context providing job configuration
         * @throws IOException if the start date is missing, blank, or invalid
         */
        @Override
        protected void setup(Context context) throws IOException {

            // Retrieve the observation start date from the Hadoop configuration.
            String configuredStart = context.getConfiguration().get("nexus.activity.start-date");

            /*
             * Reject jobs without a configured observation start date.
             */
            if(configuredStart == null || configuredStart.isBlank()) {
                throw new IOException("Set nexus.activity.start-date to the first fully observed UTC date.");
            }

            /*
             * Parse the configured date and report invalid date formats.
             */
            try { observationStart = LocalDate.parse(configuredStart); }
            catch(DateTimeParseException exception) {
                throw new IOException("Invalid nexus.activity.start-date: " + configuredStart, exception);
            }
        }

        /**
         * Parses a distinct player-day record and emits its DAU and WAU membership keys.
         *
         * <p>Each input line must contain an activity date and a player identifier
         * separated by a tab character.</p>
         *
         * @param offset the byte offset of the input line within its input split
         * @param line the input record containing the activity date and player identifier
         * @param context the Hadoop mapper context used to emit records and update counters
         * @throws IOException if an input/output error occurs
         * @throws InterruptedException if the mapper is interrupted while writing output
         */
        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

            // Split the input line into the activity date and player identifier.
            String[] fields = line.toString().split("\t", 2);

            /*
             * Count and skip malformed records or records with an empty player identifier.
             */
            if(fields.length != 2 || fields[1].isBlank()) {
                context.getCounter("ActivityMetrics", "INVALID_DISTINCT_PLAYER_ROW").increment(1);
                return;
            }

            /*
             * Parse the activity date, counting and skipping records with invalid dates.
             */
            final LocalDate activityDate;
            try { activityDate = LocalDate.parse(fields[0]); }
            catch(DateTimeParseException exception) {
                context.getCounter("ActivityMetrics", "INVALID_ACTIVITY_DATE").increment(1);
                return;
            }

            // Retrieve the player identifier from the validated input record.
            String playerId = fields[1];

            // Emit the player's daily active-user membership.
            emitDau(activityDate, playerId, context);

            // Emit the player's memberships in all applicable seven-day windows.
            emitWauWindows(activityDate, playerId, context);
        }

        /**
         * Emits the player's membership in the daily active-user metric.
         *
         * <p>The output key contains the metric type, activity date, and player
         * identifier, separated by {@code SEPARATOR}.</p>
         *
         * @param activityDate the date on which the player was active
         * @param playerId the identifier of the active player
         * @param context the Hadoop mapper context used to emit the output record
         * @throws IOException if an input/output error occurs
         * @throws InterruptedException if the mapper is interrupted while writing output
         */
        private void emitDau(LocalDate activityDate, String playerId, Context context) throws IOException, InterruptedException {

            // Build the DAU key from the metric type, activity date, and player identifier.
            outputKey.set("DAU" + SEPARATOR + activityDate + SEPARATOR + playerId);

            // Emit the key with no associated value.
            context.write(outputKey, NullWritable.get());
        }

        /**
         * Emits the player's memberships in all applicable seven-calendar-day WAU windows.
         *
         * <p>A player active on date D belongs to windows starting on dates D through
         * D-6. Windows starting before {@code observationStart} are excluded because
         * their complete observation period is unavailable.</p>
         *
         * @param activityDate the date on which the player was active
         * @param playerId the identifier of the active player
         * @param context the Hadoop mapper context used to emit the output records
         * @throws IOException if an input/output error occurs
         * @throws InterruptedException if the mapper is interrupted while writing output
         */
        private void emitWauWindows(LocalDate activityDate, String playerId, Context context)
            throws IOException, InterruptedException {

                /*
                 * Enumerate the seven possible window start dates containing the activity date.
                 */
                for(int daysBefore = 0; daysBefore < 7; daysBefore++) {

                    // Calculate the start date of the current seven-day window.
                    LocalDate windowStart = activityDate.minusDays(daysBefore);

                    // Skip windows starting before the fully observed period.
                    if(windowStart.isBefore(observationStart)) continue;

                    // Build the WAU key from the metric type, window start date, and player identifier.
                    outputKey.set("WAU" + SEPARATOR + windowStart + SEPARATOR + playerId);

                    // Emit the key with no associated value.
                    context.write(outputKey, NullWritable.get());
                }
            }
    }

// ============================================================================

    /**
     * Deduplicates repeated player memberships within a given day or WAU window.
     *
     * <p>This combiner performs local deduplication before the global distinct
     * stage. It emits one record for each key received by the combiner.</p>
     */
    public static final class DistinctWindowCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {

        /**
         * Removes duplicate memberships locally before the global distinct stage.
         *
         * @param key the composite metric, window start date, and player identifier
         * @param values the values associated with the composite key
         * @param context the Hadoop reducer context used to emit the output record
         * @throws IOException if an input/output error occurs
         * @throws InterruptedException if the combiner is interrupted while writing output
         */
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

                // Emit the key once, regardless of the number of duplicate values.
                context.write(key, NullWritable.get());
            }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Produces unique metric, window, and player records for the counting stage.
     *
     * <p>The reducer receives composite keys grouped by Hadoop and converts valid
     * keys into tab-separated output records.</p>
     */
    public static final class DistinctWindowReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        /**
         * Reusable output object for the normalized distinct membership record.
         */
        private final Text output = new Text();

        /**
         * Writes one distinct metric/window/player row from the composite key.
         *
         * @param composite the composite key containing the metric, window date,
         *                  and player identifier separated by {@code SEPARATOR}
         * @param values the values associated with the composite key
         * @param context the Hadoop reducer context used to emit the output record
         * @throws IOException if an input/output error occurs
         * @throws InterruptedException if the reducer is interrupted while writing output
         */
        @Override
        protected void reduce(Text composite, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            // Split the composite key into at most three fields.
            String[] fields = composite.toString().split(SEPARATOR, 3);

            /*
             * Verify that the composite key contains exactly the three expected fields.
             */
            if(fields.length == 3) {

                // Rebuild the normalized record with tab-separated metric, date, and player fields.
                output.set(fields[0] + "\t" + fields[1] + "\t" + fields[2]);

                // Emit the normalized record without an associated value.
                context.write(output, NullWritable.get());
            }
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Converts distinct player membership records into metric/window counting keys.
     *
     * <p>Each valid input record contributes a value of one to its metric and
     * window, allowing the reducer to calculate the number of unique players.</p>
     */
    public static final class CountMetricMapper
        extends Mapper<LongWritable, Text, Text, LongWritable> {

        /** Reusable output key containing the metric type and window date. */
        private final Text outputKey = new Text();

        /**
         * Validates a distinct membership record and maps it to a counting key.
         *
         * @param offset the byte offset of the input line within its input split
         * @param line the tab-separated record containing metric, date, and player ID
         * @param context the Hadoop mapper context used to emit records and update counters
         * @throws IOException if an input/output error occurs
         * @throws InterruptedException if the mapper is interrupted while writing output
         */
        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

            // Split the input into the metric type, window date, and player identifier.
            String[] fields = line.toString().split("\t", 3);

            /*
             * Accept only correctly structured DAU or WAU membership records.
             */
            if(fields.length != 3 || !("DAU".equals(fields[0]) || "WAU".equals(fields[0]))) {

                /* Count invalid records and exclude them from the aggregation. */
                context.getCounter("ActivityMetrics", "INVALID_DISTINCT_WINDOW_ROW").increment(1);
                return;
            }

            // Build a counting key from the metric type and window date.
            outputKey.set(fields[0] + "\t" + fields[1]);

            // Emit one contribution for the player's membership in this window.
            context.write(outputKey, new LongWritable(1));
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Sums distinct player memberships to calculate final DAU and WAU counts.
     *
     * <p>All values sharing the same metric and window key are added together
     * to produce the number of unique active players for that window.</p>
     */
    public static final class CountMetricReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        /** Reusable output value containing the aggregated player count. */
        private final LongWritable total = new LongWritable();

        /**
         * Sums player contributions for a given metric and window.
         *
         * @param key the metric type and window date
         * @param values the individual player contributions associated with the key
         * @param context the Hadoop reducer context used to emit the aggregated count
         * @throws IOException if an input/output error occurs
         * @throws InterruptedException if the reducer is interrupted while writing output
         */
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

                // Initialize the count for the current metric/window pair.
                long count = 0;

                // Add the contributions from all players belonging to this window.
                for(LongWritable value : values) { count += value.get(); }

                // Store the aggregated count in the reusable output value.
                total.set(count);

                // Emit the metric/window key and its final player count.
                context.write(key, total);
            }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Configures the first MapReduce stage, which expands and deduplicates
     * daily and weekly player memberships.
     *
     * @param configuration the Hadoop configuration used to create the job
     * @param input the input path containing distinct daily player records
     * @param output the output path for distinct metric/window/player records
     * @param observationStart the first date for which complete seven-day windows
     *                         are observable
     * @return the configured Hadoop job for the distinct membership stage
     * @throws IOException if the job cannot be configured
     */
    private static Job configureDistinctWindows(Configuration configuration, Path input,
                                                Path output, LocalDate observationStart) throws IOException {

            // Make the observation start date available to every mapper.
            configuration.set("nexus.activity.start-date", observationStart.toString());

            // Create the first MapReduce job with a descriptive name.
            Job job = Job.getInstance(configuration, "nexus-distinct-daily-and-weekly-players");

            // Specify the class containing the job implementation.
            job.setJarByClass(ActivityMetricsJob.class);

            /* Configure text input and the mapper that expands DAU/WAU memberships. */
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(ExpandWindowsMapper.class);

            /* Deduplicate memberships locally, then globally in the reducer. */
            job.setCombinerClass(DistinctWindowCombiner.class);
            job.setReducerClass(DistinctWindowReducer.class);

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
     * Configures the second MapReduce stage, which counts unique player memberships
     * for each DAU or WAU window.
     *
     * @param configuration the Hadoop configuration used to create the job
     * @param input the input path containing distinct metric/window/player records
     * @param output the output path for the final DAU and WAU counts
     * @return the configured Hadoop job for the counting stage
     * @throws IOException if the job cannot be configured
     */
    private static Job configureCounts(Configuration configuration, Path input, Path output) throws IOException {

        // Create the MapReduce job responsible for the final DAU/WAU aggregation.
        Job job = Job.getInstance(configuration, "nexus-dau-wau-counts");

        /* Specify the class containing the job implementation. */
        job.setJarByClass(ActivityMetricsJob.class);

        /* Configure text input and map each membership to a counting contribution. */
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(CountMetricMapper.class);

        /* Aggregate contributions locally and then globally. */
        job.setCombinerClass(CountMetricReducer.class);
        job.setReducerClass(CountMetricReducer.class);

        /* Define the intermediate key and value types emitted by the mapper. */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        /* Define the final key and value types written by the reducer. */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Write the aggregated metrics as text files.
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Register the input and output paths for the job. */
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // Return the configured job without executing it.
        return job;
    }

    /**
     * Executes the two MapReduce stages required to calculate daily and weekly
     * active-player counts.
     *
     * <p>The first stage expands player-day records into DAU and WAU memberships
     * and deduplicates them. The second stage aggregates those distinct memberships
     * to produce the final counts.</p>
     *
     * @param args command-line arguments in the following order:
     *             <ol>
     *               <li>Input path containing distinct daily player records.</li>
     *               <li>Output path for distinct metric/window/player records.</li>
     *               <li>Output path for final DAU/WAU metrics.</li>
     *               <li>Observation start date in {@code YYYY-MM-DD} format.</li>
     *             </ol>
     * @throws Exception if a job fails during configuration or execution
     */
    public static void main(String[] args) throws Exception {

        /*
         * Require the four command-line arguments needed by the pipeline.
         */
        if(args.length != 4) {
            System.err.println(
                "Usage: ActivityMetricsJob <distinct-day-player-input> "
                    + "<distinct-window-output> <metrics-output> "
                    + "<observation-start-date>"
            );
            System.exit(2);
        }

        // Initialize the Hadoop configuration shared by both stages.
        Configuration configuration = new Configuration();

        /*
         * Parse the observation start date and report an invalid format.
         */
        final LocalDate startDate;
        try { startDate = LocalDate.parse(args[3]); }
        catch (DateTimeParseException exception) {
            System.err.println("Observation start date must use YYYY-MM-DD.");
            System.exit(2);
            return;
        }

        // Configure and execute the first stage: expand and deduplicate memberships.
        Job distinct = configureDistinctWindows(configuration, new Path(args[0]), new Path(args[1]), startDate);

        // Stop the pipeline if the distinct-membership stage fails.
        if(!distinct.waitForCompletion(true)) System.exit(1);

        // Configure the second stage using the output of the first stage.
        Job counts = configureCounts(configuration, new Path(args[1]), new Path(args[2]));

        // Execute the counting stage and return its success or failure status.
        System.exit(counts.waitForCompletion(true) ? 0 : 1);
    }
}
