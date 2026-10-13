package fr.thesakyo.nexus.mapreduce;

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

/** Calculates fully observable Dn retention by Monday-based installation cohort. */
public final class CohortRetentionMetricsJob {
    private static final String SEPARATOR = "\u0001";

    private CohortRetentionMetricsJob() { }

    public static final class RetentionMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();
        private LocalDate observationStart;
        private LocalDate asOfDate;
        private int maxDay;

        @Override
        protected void setup(Context context) throws IOException {
            try {
                observationStart = LocalDate.parse(context.getConfiguration().get("nexus.retention.start-date"));
                asOfDate = LocalDate.parse(context.getConfiguration().get("nexus.retention.as-of-date"));
                maxDay = context.getConfiguration().getInt("nexus.retention.max-day", -1);
            } catch (DateTimeParseException | NullPointerException exception) {
                throw new IOException("Retention start and as-of dates must use YYYY-MM-DD.", exception);
            }
            if (maxDay < 0 || maxDay > 90 || asOfDate.isBefore(observationStart)) {
                throw new IOException("Retention max-day must be 0..90 and as-of-date must not precede start-date.");
            }
        }

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\t", 3);
            if (fields.length != 3 || fields[0].isBlank() || fields[1].isBlank()) {
                context.getCounter("CohortRetention", "MISSING_INSTALL_DATE_OR_COHORT").increment(1);
                return;
            }
            final LocalDate installDate;
            final LocalDate cohortWeek;
            try {
                installDate = LocalDate.parse(fields[0]);
                cohortWeek = LocalDate.parse(fields[1]);
            } catch (DateTimeParseException exception) {
                context.getCounter("CohortRetention", "INVALID_INSTALL_DATE_OR_COHORT").increment(1);
                return;
            }
            if (installDate.isBefore(observationStart) || installDate.isAfter(asOfDate)) {
                context.getCounter("CohortRetention", "INSTALL_OUTSIDE_OBSERVATION").increment(1);
                return;
            }
            Set<LocalDate> activeDates = new HashSet<>();
            if (!fields[2].isBlank()) {
                for (String date : fields[2].split(",")) {
                    try {
                        activeDates.add(LocalDate.parse(date));
                    } catch (DateTimeParseException exception) {
                        context.getCounter("CohortRetention", "INVALID_ACTIVITY_DATE").increment(1);
                    }
                }
            }
            for (int day = 0; day <= maxDay; day++) {
                LocalDate targetDate = installDate.plusDays(day);
                if (targetDate.isAfter(asOfDate)) {
                    break;
                }
                outputKey.set(cohortWeek + SEPARATOR + "D" + day);
                outputValue.set("DENOMINATOR");
                context.write(outputKey, outputValue);
                if (activeDates.contains(targetDate)) {
                    outputValue.set("ACTIVE");
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static final class RetentionReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long eligible = 0;
            long active = 0;
            for (Text value : values) {
                if ("DENOMINATOR".equals(value.toString())) {
                    eligible++;
                } else if ("ACTIVE".equals(value.toString())) {
                    active++;
                }
            }
            String[] dimensions = key.toString().split(SEPARATOR, 2);
            if (dimensions.length != 2 || eligible == 0) {
                return;
            }
            double rate = (double) active / eligible;
            outputKey.set(dimensions[0] + "\t" + dimensions[1]);
            outputValue.set(active + "\t" + eligible + "\t" + Double.toString(rate));
            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: CohortRetentionMetricsJob <player-activity-input> <output> <start-date> <as-of-date> <max-day>");
            System.exit(2);
        }
        final LocalDate startDate;
        final LocalDate asOfDate;
        final int maxDay;
        try {
            startDate = LocalDate.parse(args[2]);
            asOfDate = LocalDate.parse(args[3]);
            maxDay = Integer.parseInt(args[4]);
        } catch (DateTimeParseException | NumberFormatException exception) {
            System.err.println("Dates must use YYYY-MM-DD and max-day must be an integer.");
            System.exit(2);
            return;
        }
        if (maxDay < 0 || maxDay > 90 || asOfDate.isBefore(startDate)) {
            System.err.println("max-day must be 0..90 and as-of-date must not precede start-date.");
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        configuration.set("nexus.retention.start-date", startDate.toString());
        configuration.set("nexus.retention.as-of-date", asOfDate.toString());
        configuration.setInt("nexus.retention.max-day", maxDay);
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
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
