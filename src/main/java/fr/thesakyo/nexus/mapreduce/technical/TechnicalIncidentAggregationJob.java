package fr.thesakyo.nexus.mapreduce.technical;

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

/**
 * Sums player-level technical counts by day, game version, platform, and GPU vendor.
 *
 * <p>The input is the joined output of {@link TechnicalIncidentsByProfileJob}. The output
 * contains sessions, crashes, disconnects, and the crash-to-session ratio for each dimension.</p>
 */
public final class TechnicalIncidentAggregationJob {

    /** Prevents instantiation of this command-line job class. */
    private TechnicalIncidentAggregationJob() {}

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Validates joined rows and groups their three event counts by the four dimensions.
     */
    public static final class CountMapper extends Mapper<LongWritable, Text, Text, Text> {

        /** Reusable Hadoop output key containing the four aggregation dimensions. */
        private final Text dimensionKey = new Text();

        /** Reusable Hadoop output value containing the three event counts. */
        private final Text counts = new Text();

        /**
         * Validates a joined count row and emits the dimensions with its three counters.
         *
         * @param offset the input line offset
         * @param line the tab-separated joined count row
         * @param context the mapper context used to emit records and quality counters
         * @throws IOException if writing the mapped record fails
         * @throws InterruptedException if the mapper is interrupted
         */
        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

            // Split the input row into its tab-separated fields.
            String[] fields = splitFields(line);

            /*
             * Reject rows that do not contain the expected dimensions and count fields.
             */
            if(!hasExpectedFieldCount(fields)) {
                context.getCounter(
                    "TechnicalIncidentAggregation",
                    "INVALID_JOINED_ROW"
                ).increment(1);
                return;
            }

            /*
             * Reject rows whose event counts cannot be parsed as long integers.
             */
            if(!hasValidCounts(fields)) {
                context.getCounter(
                    "TechnicalIncidentAggregation",
                    "INVALID_COUNT"
                ).increment(1);
                return;
            }

            // Build the aggregation key from the day, game version, platform, and GPU vendor.
            dimensionKey.set(String.join("\t", fields[0], fields[1], fields[2], fields[3]));

            // Group the three event counts into a single tab-separated value.
            counts.set(String.join("\t", fields[4], fields[5], fields[6]));

            // Emit the dimensions and their associated event counts.
            context.write(dimensionKey, counts);
        }

        /**
         * Returns whether all three event counts parse as signed long integers.
         *
         * @param fields the split input row fields
         * @return {@code true} if all three counts are valid long integers; otherwise {@code false}
         */
        private boolean hasValidCounts(String[] fields) {

            // Validate each event count using the long integer parser.
            try { Long.parseLong(fields[4]); Long.parseLong(fields[5]); Long.parseLong(fields[6]); return true; }

            // Report invalid counts to the caller for quality-counter handling.
            catch (NumberFormatException exception) { return false; }
        }

        /**
         * Checks the four dimension columns plus the three event-count columns.
         *
         * @param fields the split input row fields
         * @return {@code true} if the row contains exactly seven fields
         */
        private boolean hasExpectedFieldCount(String[] fields) {

            // Require four dimensions followed by three event counts.
            return fields.length == 7;
        }

        /**
         * Splits a tab-separated row while preserving empty trailing columns.
         *
         * @param line the input row
         * @return the row fields, including empty trailing fields
         */
        private String[] splitFields(Text line) {

            // Preserve trailing empty fields so malformed rows can be detected.
            return line.toString().split("\t", -1);
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Adds counts for one day/version/platform/GPU dimension and calculates its crash rate.
     */
    public static final class SumReducer extends Reducer<Text, Text, Text, Text> {

        /** Reusable Hadoop output value containing the aggregated metrics. */
        private final Text result = new Text();

        /**
         * Sums sessions and incidents for one dimension and calculates crashes per session.
         *
         * @param dimension the day, game version, platform, and GPU vendor grouping key
         * @param values the event counts emitted by the mapper for this dimension
         * @param context the reducer context used to write the aggregated result
         * @throws IOException if writing the result fails
         * @throws InterruptedException if the reducer is interrupted
         */
        @Override
        protected void reduce(Text dimension, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Total number of sessions for this dimension.
            long sessions = 0;

            // Total number of crashes for this dimension.
            long crashes = 0;

            // Total number of disconnects for this dimension.
            long disconnects = 0;

            /*
             * Aggregate the session, crash, and disconnect counts from all mapper values
             * associated with this dimension.
             */
            for(Text value : values) {

                // Parse the three event counts from the current mapper value.
                long[] counts = parseCounts(value);

                /*
                 * Add each event count to its corresponding total.
                 */
                sessions += counts[0];
                crashes += counts[1];
                disconnects += counts[2];
            }

            // Format the aggregated totals and the crash-to-session ratio.
            result.set(formatCounts(sessions, crashes, disconnects));

            // Write the final metrics for this dimension.
            context.write(dimension, result);
        }

        /**
         * Parses the sessions, crashes, and disconnects carried by one mapper value.
         *
         * @param value the tab-separated mapper value
         * @return an array containing sessions, crashes, and disconnects, in that order
         */
        private long[] parseCounts(Text value) {

            // Split the mapper value while preserving empty trailing fields.
            String[] fields = value.toString().split("\t", -1);

            // Return zero counts if the value does not contain exactly three fields.
            if(fields.length != 3) return new long[3];

            // Convert the three fields into their numeric representations.
            return new long[] { Long.parseLong(fields[0]), Long.parseLong(fields[1]), Long.parseLong(fields[2]) };
        }

        /**
         * Formats totals and uses {@code NA} when a crash rate has a zero denominator.
         *
         * @param sessions the total number of sessions
         * @param crashes the total number of crashes
         * @param disconnects the total number of disconnects
         * @return tab-separated totals followed by the crash-to-session ratio
         */
        private String formatCounts(long sessions, long crashes, long disconnects) {

            // Avoid division by zero when no sessions were recorded.
            String crashRate = sessions == 0 ? "NA" : Double.toString((double) crashes / sessions);

            // Return the aggregated metrics as tab-separated output fields.
            return sessions + "\t" + crashes + "\t" + disconnects + "\t" + crashRate;
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Configures and submits the final technical aggregation job.
     *
     * @param args joined-count input path and new analytics output path
     * @throws Exception if argument validation, Hadoop setup, or job execution fails
     */
    public static void main(String[] args) throws Exception {

        /*
         * Validate the number of command-line arguments.
         */
        if(args.length != 2) {
            System.err.println(
                "Usage: TechnicalIncidentAggregationJob <joined-counts-input> <analytics-output>"
            );
            System.exit(2);
        }

        // Create the Hadoop job with its descriptive name.
        Job job = Job.getInstance(new Configuration(), "nexus-technical-incident-profile-metrics");

        /* Configure the job class and input format. */
        job.setJarByClass(TechnicalIncidentAggregationJob.class);
        job.setInputFormatClass(TextInputFormat.class);

        /* Configure the mapper, reducer, and intermediate key-value types. */
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        /* Configure the final output types and format. */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set the joined count input path.
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set the destination for the aggregated technical metrics.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Return a process exit code based on the job execution result.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
