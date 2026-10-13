package fr.thesakyo.nexus.mapreduce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/** Joins crash/disconnect/session events to MongoDB player profiles via a reduce-side join. */
public final class TechnicalIncidentsByProfileJob {
    private static final String KEY_SEPARATOR = "\u0001";
    private static final ObjectMapper JSON = new ObjectMapper();

    private TechnicalIncidentsByProfileJob() { }

    public enum QualityCounters {
        PLAYER_LINES,
        PLAYER_RECORDS,
        PLAYER_RECORDS_WITHOUT_ID,
        MALFORMED_PLAYER_JSON,
        TELEMETRY_LINES,
        SELECTED_TELEMETRY_EVENTS,
        TELEMETRY_WITHOUT_PLAYER_ID,
        TELEMETRY_INVALID,
        PLAYERS_WITHOUT_PROFILE,
        DUPLICATE_PLAYER_RECORDS
    }

    public static final class PlayerMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            context.getCounter(QualityCounters.PLAYER_LINES).increment(1);
            final JsonNode player;
            try {
                player = JSON.readTree(line.toString());
            } catch (IOException | RuntimeException exception) {
                context.getCounter(QualityCounters.MALFORMED_PLAYER_JSON).increment(1);
                return;
            }
            String playerId = TelemetryEvent.text(player, "_id");
            if (playerId.isBlank()) {
                context.getCounter(QualityCounters.PLAYER_RECORDS_WITHOUT_ID).increment(1);
                return;
            }
            JsonNode profile = player.path("profile");
            outputKey.set(playerId + KEY_SEPARATOR + "0");
            outputValue.set("P\t" + clean(TelemetryEvent.text(profile, "platform"))
                    + "\t" + clean(TelemetryEvent.text(profile, "gpu_vendor"))
                    + "\t" + clean(TelemetryEvent.text(profile, "os")));
            context.write(outputKey, outputValue);
            context.getCounter(QualityCounters.PLAYER_RECORDS).increment(1);
        }
    }

    public static final class IncidentMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            context.getCounter(QualityCounters.TELEMETRY_LINES).increment(1);
            final TelemetryEvent event;
            try {
                event = TelemetryEvent.parse(line.toString());
            } catch (TelemetryEvent.InvalidRecordException exception) {
                context.getCounter(QualityCounters.TELEMETRY_INVALID).increment(1);
                return;
            }
            if (!("session_start".equals(event.eventType())
                    || "crash".equals(event.eventType())
                    || "disconnect".equals(event.eventType()))) {
                return;
            }
            context.getCounter(QualityCounters.SELECTED_TELEMETRY_EVENTS).increment(1);
            if (event.playerId().isBlank()) {
                context.getCounter(QualityCounters.TELEMETRY_WITHOUT_PLAYER_ID).increment(1);
                return;
            }
            String day = event.timestamp().atOffset(ZoneOffset.UTC).toLocalDate().toString();
            outputKey.set(event.playerId() + KEY_SEPARATOR + "1");
            outputValue.set("E\t" + day + "\t" + clean(event.eventType()) + "\t"
                    + clean(nonEmpty(event.gameVersion(), "unknown")));
            context.write(outputKey, outputValue);
        }
    }

    public static final class PlayerPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int partitions) {
            String raw = key.toString();
            int separator = raw.indexOf(KEY_SEPARATOR);
            String playerId = separator < 0 ? raw : raw.substring(0, separator);
            return (playerId.hashCode() & Integer.MAX_VALUE) % partitions;
        }
    }

    /** Groups the composite keys by player while retaining the profile-before-event secondary sort. */
    public static final class PlayerGroupingComparator extends WritableComparator {
        public PlayerGroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable left, WritableComparable right) {
            return playerPart(left.toString()).compareTo(playerPart(right.toString()));
        }

        @Override
        public int compare(byte[] left, int leftStart, int leftLength,
                           byte[] right, int rightStart, int rightLength) {
            return playerPart(decode(left, leftStart, leftLength))
                    .compareTo(playerPart(decode(right, rightStart, rightLength)));
        }

        private static String decode(byte[] bytes, int start, int length) {
            DataInputBuffer input = new DataInputBuffer();
            input.reset(bytes, start, length);
            Text text = new Text();
            try {
                text.readFields(input);
                return text.toString();
            } catch (IOException exception) {
                throw new IllegalArgumentException("Invalid serialized Text key", exception);
            }
        }

        private static String playerPart(String composite) {
            int separator = composite.indexOf(KEY_SEPARATOR);
            return separator < 0 ? composite : composite.substring(0, separator);
        }
    }

    public static final class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void reduce(Text compositeKey, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String playerId = PlayerGroupingComparator.playerPart(compositeKey.toString());
            String platform = "unknown";
            String gpuVendor = "unknown";
            boolean profileSeen = false;
            boolean duplicateProfile = false;
            Map<String, long[]> countsByDimension = new HashMap<>();

            for (Text value : values) {
                String[] fields = value.toString().split("\t", -1);
                if (fields.length == 0) {
                    continue;
                }
                if ("P".equals(fields[0])) {
                    if (profileSeen) {
                        context.getCounter(QualityCounters.DUPLICATE_PLAYER_RECORDS).increment(1);
                        duplicateProfile = true;
                        continue;
                    }
                    profileSeen = true;
                    platform = fields.length > 1 ? nonEmpty(fields[1], "unknown") : "unknown";
                    gpuVendor = fields.length > 2 ? nonEmpty(fields[2], "unknown") : "unknown";
                    continue;
                }
                if (!"E".equals(fields[0]) || fields.length < 4) {
                    continue;
                }
                String day = fields[1];
                String eventType = fields[2];
                String version = nonEmpty(fields[3], "unknown");
                String dimension = day + "\t" + version + "\t" + platform + "\t" + gpuVendor;
                long[] counts = countsByDimension.computeIfAbsent(dimension, ignored -> new long[3]);
                switch (eventType) {
                    case "session_start" -> counts[0]++;
                    case "crash" -> counts[1]++;
                    case "disconnect" -> counts[2]++;
                    default -> { }
                }
            }
            if (!profileSeen) {
                context.getCounter(QualityCounters.PLAYERS_WITHOUT_PROFILE).increment(1);
                return;
            }
            if (duplicateProfile) {
                return;
            }
            for (Map.Entry<String, long[]> entry : countsByDimension.entrySet()) {
                long[] counts = entry.getValue();
                outputKey.set(entry.getKey());
                outputValue.set(counts[0] + "\t" + counts[1] + "\t" + counts[2]);
                context.write(outputKey, outputValue);
            }
        }
    }

    private static String clean(String value) {
        return value == null ? "" : value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ').trim();
    }

    private static String nonEmpty(String value, String fallback) {
        String cleaned = clean(value);
        return cleaned.isBlank() ? fallback : cleaned;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TechnicalIncidentsByProfileJob <players-jsonl> <telemetry-jsonl> <joined-output>");
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "nexus-technical-incidents-by-player-profile");
        job.setJarByClass(TechnicalIncidentsByProfileJob.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(PlayerPartitioner.class);
        job.setGroupingComparatorClass(PlayerGroupingComparator.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PlayerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, IncidentMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
