package fr.thesakyo.nexus.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Produces one row per UTC day and distinct player with a session_start event. */
public final class DailyActivePlayersJob {
    private static final String SEPARATOR = "\u0001";

    private DailyActivePlayersJob() { }

    public enum QualityCounters {
        INPUT_LINES,
        ACCEPTED_SESSION_STARTS,
        IGNORED_EVENT_TYPES,
        MISSING_PLAYER_ID,
        MALFORMED_JSON,
        NOT_AN_OBJECT,
        MISSING_EVENT_TYPE,
        MISSING_TIMESTAMP,
        INVALID_TIMESTAMP
    }

    public static final class ActivePlayerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text outputKey = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            context.getCounter(QualityCounters.INPUT_LINES).increment(1);
            final TelemetryEvent event;
            try {
                event = TelemetryEvent.parse(line.toString());
            } catch (TelemetryEvent.InvalidRecordException exception) {
                context.getCounter(QualityCounters.valueOf(exception.reason().name())).increment(1);
                return;
            }
            if (!"session_start".equals(event.eventType())) {
                context.getCounter(QualityCounters.IGNORED_EVENT_TYPES).increment(1);
                return;
            }
            if (event.playerId().isBlank()) {
                context.getCounter(QualityCounters.MISSING_PLAYER_ID).increment(1);
                return;
            }

            String day = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC).format(event.timestamp());
            outputKey.set(day + SEPARATOR + event.playerId());
            context.write(outputKey, NullWritable.get());
            context.getCounter(QualityCounters.ACCEPTED_SESSION_STARTS).increment(1);
        }
    }

    public static final class DistinctPlayerCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static final class DistinctPlayerReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        private final Text output = new Text();

        @Override
        protected void reduce(Text compositeKey, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            String[] fields = compositeKey.toString().split(SEPARATOR, 2);
            if (fields.length != 2) {
                return;
            }
            output.set(fields[0] + "\t" + fields[1]);
            context.write(output, NullWritable.get());
        }
    }

    public static Job configure(Configuration configuration, Path input, Path output) throws IOException {
        Job job = Job.getInstance(configuration, "nexus-daily-active-players");
        job.setJarByClass(DailyActivePlayersJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ActivePlayerMapper.class);
        job.setCombinerClass(DistinctPlayerCombiner.class);
        job.setReducerClass(DistinctPlayerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DailyActivePlayersJob <telemetry-jsonl-input> <distinct-day-player-output>");
            System.exit(2);
        }
        Job job = configure(new Configuration(), new Path(args[0]), new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
