package fr.thesakyo.nexus.mapreduce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.time.DayOfWeek;
import java.util.TreeSet;

/** Joins player install dates with distinct session-start activity dates. */
public final class PlayerCohortJoinJob {
    private static final String KEY_SEPARATOR = "\u0001";
    private static final ObjectMapper JSON = new ObjectMapper();

    private PlayerCohortJoinJob() { }

    public enum QualityCounters {
        PLAYER_LINES,
        PLAYER_RECORDS_WITHOUT_ID,
        PLAYER_RECORDS_WITHOUT_INSTALL_DATE,
        MALFORMED_PLAYER_JSON,
        TELEMETRY_LINES,
        TELEMETRY_INVALID,
        TELEMETRY_WITHOUT_PLAYER_ID,
        JOINED_PLAYERS,
        ACTIVITY_WITHOUT_PLAYER_RECORD,
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
            String installDate = "";
            JsonNode createdAt = player.get("created_at");
            if (createdAt != null && !createdAt.isNull()) {
                try {
                    installDate = TelemetryEvent.parseTimestamp(createdAt)
                            .atOffset(ZoneOffset.UTC).toLocalDate().toString();
                } catch (RuntimeException exception) {
                    context.getCounter(QualityCounters.PLAYER_RECORDS_WITHOUT_INSTALL_DATE).increment(1);
                }
            } else {
                context.getCounter(QualityCounters.PLAYER_RECORDS_WITHOUT_INSTALL_DATE).increment(1);
            }
            outputKey.set(playerId + KEY_SEPARATOR + "0");
            outputValue.set("P\t" + installDate);
            context.write(outputKey, outputValue);
        }
    }

    public static final class ActivityMapper extends Mapper<LongWritable, Text, Text, Text> {
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
            if (!"session_start".equals(event.eventType())) {
                return;
            }
            if (event.playerId().isBlank()) {
                context.getCounter(QualityCounters.TELEMETRY_WITHOUT_PLAYER_ID).increment(1);
                return;
            }
            LocalDate activityDate = event.timestamp().atOffset(ZoneOffset.UTC).toLocalDate();
            outputKey.set(event.playerId() + KEY_SEPARATOR + "1");
            outputValue.set("A\t" + activityDate);
            context.write(outputKey, outputValue);
        }
    }

    public static final class CohortJoinReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void reduce(Text composite, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String installDateText = "";
            boolean playerSeen = false;
            boolean duplicatePlayer = false;
            TreeSet<LocalDate> activityDates = new TreeSet<>();
            for (Text value : values) {
                String[] fields = value.toString().split("\t", -1);
                if (fields.length < 2) {
                    continue;
                }
                if ("P".equals(fields[0])) {
                    if (playerSeen) {
                        context.getCounter(QualityCounters.DUPLICATE_PLAYER_RECORDS).increment(1);
                        duplicatePlayer = true;
                        continue;
                    }
                    playerSeen = true;
                    installDateText = fields[1];
                } else if ("A".equals(fields[0])) {
                    try {
                        activityDates.add(LocalDate.parse(fields[1]));
                    } catch (RuntimeException ignored) {
                        // Invalid dates are already excluded by the telemetry parser.
                    }
                }
            }
            if (!playerSeen) {
                context.getCounter(QualityCounters.ACTIVITY_WITHOUT_PLAYER_RECORD).increment(1);
                return;
            }
            if (duplicatePlayer) {
                return;
            }
            String cohort = "";
            if (!installDateText.isBlank()) {
                LocalDate installDate = LocalDate.parse(installDateText);
                cohort = installDate.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).toString();
            }
            String activeDates = activityDates.stream().map(LocalDate::toString)
                    .collect(java.util.stream.Collectors.joining(","));
            outputKey.set(installDateText + "\t" + cohort);
            outputValue.set(activeDates);
            context.write(outputKey, outputValue);
            context.getCounter(QualityCounters.JOINED_PLAYERS).increment(1);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PlayerCohortJoinJob <players-jsonl> <telemetry-jsonl> <player-activity-output>");
            System.exit(2);
        }
        Job job = Job.getInstance(new Configuration(), "nexus-player-cohort-activity-join");
        job.setJarByClass(PlayerCohortJoinJob.class);
        job.setPartitionerClass(TechnicalIncidentsByProfileJob.PlayerPartitioner.class);
        job.setGroupingComparatorClass(TechnicalIncidentsByProfileJob.PlayerGroupingComparator.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(CohortJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PlayerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ActivityMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
