package fr.thesakyo.nexus.mapreduce.retention;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import java.util.HashSet;
import java.util.Set;

/**
 * Calculates fully observable exact-day retention by Monday-based installation cohort.
 *
 * <p>Only installation cohorts and Dn values observable by the configured as-of date
 * are emitted. Each output row contains the cohort date, retention day, active players,
 * eligible players, and retention rate.</p>
 */
public final class CohortRetentionMetricsJob {

    /** Separates cohort and Dn fields in internal Hadoop keys. */
    private static final String SEPARATOR = "\u0001";

    /** Prevents instantiation of this command-line job class. */
    private CohortRetentionMetricsJob() {}

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Reads player-level cohort facts and emits one numerator/denominator pair per eligible Dn.
     */
    public static final class RetentionMapper extends Mapper<LongWritable, Text, Text, Text> {

        /** Reusable Hadoop output key for the cohort and retention day. */
        private final Text outputKey = new Text();

        /** Reusable Hadoop output value for the retention metrics. */
        private final Text outputValue = new Text();

        /** First date of the observation period. */
        private LocalDate observationStart;

        /** Last date for which retention can be observed. */
        private LocalDate asOfDate;

        /** Maximum retention day to calculate, from D0 to D90. */
        private int maxDay;

        /**
         * Loads date bounds and the maximum Dn from the Hadoop job configuration.
         *
         * @param context the mapper context containing the job configuration
         * @throws IOException if the date configuration is missing, invalid, or inconsistent
         */
        @Override
        protected void setup(Context context) throws IOException {

            /*
             * Read the observation period and maximum retention day.
             */
            try {
                observationStart = LocalDate.parse(context.getConfiguration().get("nexus.retention.start-date"));
                asOfDate = LocalDate.parse(context.getConfiguration().get("nexus.retention.as-of-date"));
                maxDay = context.getConfiguration().getInt("nexus.retention.max-day", -1);
            } catch(DateTimeParseException | NullPointerException exception) {
                throw new IOException(
                    "Retention start and as-of dates must use YYYY-MM-DD.",
                    exception);
            }

            /* Validate the retention window before processing any player records.
             * The configured maximum Dn is limited to 90 days.
             */
            if(maxDay < 0 || maxDay > 90 || asOfDate.isBefore(observationStart)) {
                throw new IOException("Retention max-day must be 0..90 and as-of-date must not precede start-date.");
            }
        }

        /**
         * Validates one player fact and emits contributions for each observable Dn.
         *
         * @param offset the input line offset
         * @param line the tab-separated player activity record
         * @param context the mapper context used to emit contributions and quality counters
         * @throws IOException if writing a contribution fails
         * @throws InterruptedException if the mapper is interrupted
         */
        @Override
        protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {

            /*
             * Parse the player record and skip records that fail validation.
             */
            PlayerRetention player = parsePlayer(line, context);
            if(player == null) return;

            /*
             * Exclude installations outside the configured observation period.
             */
            if(player.installDate().isBefore(observationStart) || player.installDate().isAfter(asOfDate)) {
                context.getCounter(
                    "CohortRetention",
                    "INSTALL_OUTSIDE_OBSERVATION"
                ).increment(1);
                return;
            }

            // Emit denominator and activity contributions for observable retention days.
            emitEligibleDays(player, context);
        }

        /**
         * Parses the tab-separated player fact and counts missing or malformed date fields.
         *
         * @param line the player activity record
         * @param context the mapper context used to increment quality counters
         * @return the parsed player record, or {@code null} if required fields are invalid
         */
        private PlayerRetention parsePlayer(Text line, Context context) {

            // Split the record into installation date, cohort week, and activity dates.
            String[] fields = line.toString().split("\t", 3);

            /*
             * Reject records missing the installation date or cohort week.
             */
            if(fields.length != 3 || fields[0].isBlank() || fields[1].isBlank()) {
                context.getCounter(
                    "CohortRetention",
                    "MISSING_INSTALL_DATE_OR_COHORT"
                ).increment(1);
                return null;
            }

            /*
             * Parse the installation date and cohort week, then collect valid activity dates.
             * Invalid activity dates are counted separately by parseActivityDates().
             */
            try {
                return new PlayerRetention(
                    LocalDate.parse(fields[0]),
                    LocalDate.parse(fields[1]),
                    parseActivityDates(fields[2], context));
            } catch(DateTimeParseException exception) {
                context.getCounter(
                    "CohortRetention",
                    "INVALID_INSTALL_DATE_OR_COHORT"
                ).increment(1);
                return null;
            }
        }

        /**
         * Parses the comma-separated activity dates while counting invalid entries.
         *
         * @param rawDates the comma-separated activity dates
         * @param context the mapper context used to increment quality counters
         * @return the set of successfully parsed activity dates
         */
        private Set<LocalDate> parseActivityDates(String rawDates, Context context) {

            // Use a set to avoid counting the same activity date more than once per player.
            Set<LocalDate> dates = new HashSet<>();

            // An empty activity field represents a player with no recorded activity dates.
            if(rawDates.isBlank()) return dates;

            /*
             * Parse each activity date independently so one invalid value does not discard the record.
             */
            for(String value : rawDates.split(",")) {

                /*
                 * Parse the activity date and add it to the collection.
                 */
                try {dates.add(LocalDate.parse(value)); }
                catch(DateTimeParseException exception) {
                    context.getCounter(
                        "CohortRetention",
                        "INVALID_ACTIVITY_DATE"
                    ).increment(1);
                }
            }

            // Return the set of successfully parsed activity dates.
            return dates;
        }

        /**
         * Emits one denominator and, when applicable, one active contribution per observable Dn.
         *
         * @param player the parsed player activity record
         * @param context the mapper context used to emit contributions
         * @throws IOException if writing a contribution fails
         * @throws InterruptedException if the mapper is interrupted
         */
        private void emitEligibleDays(PlayerRetention player, Context context) throws IOException, InterruptedException {

            /*
             * Evaluate each retention day up to the configured maximum.
             */
            for(int day = 0; day <= maxDay; day++) {

                // Target calendar date for the current retention day.
                LocalDate targetDate = player.installDate().plusDays(day);

                // Stop when the target day falls beyond the observation cutoff.
                if(targetDate.isAfter(asOfDate)) break;

                // Every observable player-day contributes to the retention denominator.
                emitContribution(player.cohortWeek(), day, "DENOMINATOR", context);

                /*
                 * Active players also contribute to the retention numerator.
                 */
                if(player.activeDates().contains(targetDate)) {
                    emitContribution(player.cohortWeek(), day, "ACTIVE", context);
                }
            }
        }

        /**
         * Writes one reducer contribution keyed by cohort week and exact retention day.
         *
         * @param cohort the Monday-based installation cohort date
         * @param day the exact retention day, such as D1 or D7
         * @param contribution the contribution type, either {@code DENOMINATOR} or {@code ACTIVE}
         * @param context the mapper context used to emit the key-value pair
         * @throws IOException if writing the contribution fails
         * @throws InterruptedException if the mapper is interrupted
         */
        private void emitContribution(LocalDate cohort, int day, String contribution, Context context)
            throws IOException, InterruptedException {

                /* Combine the cohort date and retention day into one internal key. */
                outputKey.set(cohort + SEPARATOR + "D" + day);
                outputValue.set(contribution);

                // Forward the contribution to the reducer.
                context.write(outputKey, outputValue);
            }

        /**
         * Parsed player fields needed for the exact-day retention calculation.
         *
         * @param installDate the player's installation date
         * @param cohortWeek the Monday-based installation cohort date
         * @param activeDates the dates on which the player was active
         */
        private record PlayerRetention(
            LocalDate installDate,
            LocalDate cohortWeek,
            Set<LocalDate> activeDates) {
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Counts eligible players and active players, then writes the retention ratio.
     */
    public static final class RetentionReducer extends Reducer<Text, Text, Text, Text> {

        /** Reusable Hadoop output key for the cohort and retention day. */
        private final Text outputKey = new Text();

        /** Reusable Hadoop output value for the retention metrics. */
        private final Text outputValue = new Text();

        /**
         * Counts active and eligible player contributions and computes the retention ratio.
         *
         * @param key the internal key containing the cohort date and retention day
         * @param values the player contributions associated with the key
         * @param context the reducer context used to write the result
         * @throws IOException if writing the result fails
         * @throws InterruptedException if the reducer is interrupted
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

                // Total number of players eligible for the retention calculation.
                long eligible = 0;

                // Number of eligible players who were active on the target retention day.
                long active = 0;

                /*
                 * Aggregate the denominator and numerator contributions for this cohort and Dn.
                 */
                for(Text value : values) {
                    if("DENOMINATOR".equals(value.toString())) eligible++;
                    else if("ACTIVE".equals(value.toString())) active++;
                }

                // Separate the cohort date and retention day from the internal key.
                String[] dimensions = key.toString().split(SEPARATOR, 2);

                // Skip malformed keys and groups without eligible players.
                if(dimensions.length != 2 || eligible == 0) return;

                // Calculate the exact-day retention rate.
                double rate = (double) active / eligible;

                /* Format the output as cohort, Dn, active count, eligible count, and rate. */
                outputKey.set(dimensions[0] + "\t" + dimensions[1]);
                outputValue.set(active + "\t" + eligible + "\t" + Double.toString(rate));
                context.write(outputKey, outputValue);
            }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Command-line entry point.
     *
     * <p>Usage: {@code <player-activity-input> <output> <start-date> <as-of-date> <max-day>}</p>
     *
     * @param args player facts path, output path, observation start, as-of date, and maximum Dn
     * @throws Exception if Hadoop job setup or execution fails
     */
    public static void main(String[] args) throws Exception {

        /*
         * Validate the number of command-line arguments.
         */
        if(args.length != 5) {
            System.err.println(
                "Usage: CohortRetentionMetricsJob <player-activity-input> <output> <start-date> <as-of-date> <max-day>"
            );
            System.exit(2);
        }

        /*
         * Parse the observation period and maximum retention day.
         */
        final LocalDate startDate;
        final LocalDate asOfDate;
        final int maxDay;

        /*
         * Parse the observation period and maximum retention day.
         */
        try {
            startDate = LocalDate.parse(args[2]);
            asOfDate = LocalDate.parse(args[3]);
            maxDay = Integer.parseInt(args[4]);
        } catch(DateTimeParseException | NumberFormatException exception) {
            System.err.println("Dates must use YYYY-MM-DD and max-day must be an integer.");
            System.exit(2);
            return;
        }

        /*
         * Validate the retention window before configuring the Hadoop job.
         */
        if(maxDay < 0 || maxDay > 90 || asOfDate.isBefore(startDate)) {
            System.err.println("max-day must be 0..90 and as-of-date must not precede start-date.");
            System.exit(2);
        }

        /* Store the retention parameters in the Hadoop configuration. */
        Configuration configuration = new Configuration();
        configuration.set("nexus.retention.start-date", startDate.toString());
        configuration.set("nexus.retention.as-of-date", asOfDate.toString());
        configuration.setInt("nexus.retention.max-day", maxDay);

        /* Create and configure the MapReduce job. */
        Job job = Job.getInstance(configuration, "nexus-cohort-retention-daily");
        job.setJarByClass(CohortRetentionMetricsJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(RetentionMapper.class);
        job.setReducerClass(RetentionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Configure the input and output paths. */
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Return a process exit code based on the job execution result.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
