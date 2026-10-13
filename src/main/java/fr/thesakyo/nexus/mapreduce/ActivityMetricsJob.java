package fr.thesakyo.nexus.mapreduce;

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

/** Builds DAU and rolling seven-UTC-day WAU from DailyActivePlayersJob output. */
public final class ActivityMetricsJob {
    private static final String SEPARATOR = "\u0001";

    private ActivityMetricsJob() { }

    public static final class ExpandWindowsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text outputKey = new Text();
        private LocalDate observationStart;

        @Override
        protected void setup(Context context) throws IOException {
            String configuredStart = context.getConfiguration().get("nexus.activity.start-date");
            if (configuredStart == null || configuredStart.isBlank()) {
                throw new IOException("Set nexus.activity.start-date to the first fully observed UTC date.");
            }
            try {
                observationStart = LocalDate.parse(configuredStart);
            } catch (DateTimeParseException exception) {
                throw new IOException("Invalid nexus.activity.start-date: " + configuredStart, exception);
            }
        }

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\t", 2);
            if (fields.length != 2 || fields[1].isBlank()) {
                context.getCounter("ActivityMetrics", "INVALID_DISTINCT_PLAYER_ROW").increment(1);
                return;
            }
            final LocalDate activityDate;
            try {
                activityDate = LocalDate.parse(fields[0]);
            } catch (DateTimeParseException exception) {
                context.getCounter("ActivityMetrics", "INVALID_ACTIVITY_DATE").increment(1);
                return;
            }
            String playerId = fields[1];
            outputKey.set("DAU" + SEPARATOR + activityDate + SEPARATOR + playerId);
            context.write(outputKey, NullWritable.get());

            // A player active on day D is included in windows starting D through D-6.
            for (int daysBefore = 0; daysBefore < 7; daysBefore++) {
                LocalDate windowStart = activityDate.minusDays(daysBefore);
                if (windowStart.isBefore(observationStart)) {
                    continue;
                }
                outputKey.set("WAU" + SEPARATOR + windowStart + SEPARATOR + playerId);
                context.write(outputKey, NullWritable.get());
            }
        }
    }

    public static final class DistinctWindowCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static final class DistinctWindowReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        private final Text output = new Text();

        @Override
        protected void reduce(Text composite, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            String[] fields = composite.toString().split(SEPARATOR, 3);
            if (fields.length == 3) {
                output.set(fields[0] + "\t" + fields[1] + "\t" + fields[2]);
                context.write(output, NullWritable.get());
            }
        }
    }

    public static final class CountMetricMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text outputKey = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\t", 3);
            if (fields.length != 3 || !("DAU".equals(fields[0]) || "WAU".equals(fields[0]))) {
                context.getCounter("ActivityMetrics", "INVALID_DISTINCT_WINDOW_ROW").increment(1);
                return;
            }
            outputKey.set(fields[0] + "\t" + fields[1]);
            context.write(outputKey, new LongWritable(1));
        }
    }

    public static final class CountMetricReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable total = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            total.set(count);
            context.write(key, total);
        }
    }

    private static Job configureDistinctWindows(Configuration configuration, Path input, Path output,
                                                LocalDate observationStart)
            throws IOException {
        configuration.set("nexus.activity.start-date", observationStart.toString());
        Job job = Job.getInstance(configuration, "nexus-distinct-daily-and-weekly-players");
        job.setJarByClass(ActivityMetricsJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ExpandWindowsMapper.class);
        job.setCombinerClass(DistinctWindowCombiner.class);
        job.setReducerClass(DistinctWindowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    private static Job configureCounts(Configuration configuration, Path input, Path output) throws IOException {
        Job job = Job.getInstance(configuration, "nexus-dau-wau-counts");
        job.setJarByClass(ActivityMetricsJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(CountMetricMapper.class);
        job.setCombinerClass(CountMetricReducer.class);
        job.setReducerClass(CountMetricReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: ActivityMetricsJob <distinct-day-player-input> <distinct-window-output> <metrics-output> <observation-start-date>");
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        final LocalDate startDate;
        try {
            startDate = LocalDate.parse(args[3]);
        } catch (DateTimeParseException exception) {
            System.err.println("Observation start date must use YYYY-MM-DD.");
            System.exit(2);
            return;
        }
        Job distinct = configureDistinctWindows(configuration, new Path(args[0]), new Path(args[1]), startDate);
        if (!distinct.waitForCompletion(true)) {
            System.exit(1);
        }
        Job counts = configureCounts(configuration, new Path(args[1]), new Path(args[2]));
        System.exit(counts.waitForCompletion(true) ? 0 : 1);
    }
}
