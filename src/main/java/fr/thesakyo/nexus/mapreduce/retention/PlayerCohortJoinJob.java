package fr.thesakyo.nexus.mapreduce.retention;

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
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.time.DayOfWeek;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Joins player creation dates with distinct {@code session_start} activity dates.
 *
 * <p>Output is a player-level TSV fact containing the installation date, Monday-based
 * cohort date, and comma-separated unique UTC activity dates. Player records are
 * secondary-sorted before events so the reducer can stream each player's activity
 * without requiring a full source-side join.</p>
 */
public final class PlayerCohortJoinJob {

    /** Prevents instantiation of this command-line job class. */
    private PlayerCohortJoinJob() {}

    /**
     * Counters for records that cannot be used in the cohort join.
     */
    public enum QualityCounters {

        /** Player snapshot lines read by the profile mapper. */
        PLAYER_LINES,

        /** Player records without a usable source identifier. */
        PLAYER_RECORDS_WITHOUT_ID,

        /** Identified players with no supported creation timestamp. */
        PLAYER_RECORDS_WITHOUT_INSTALL_DATE,

        /** Player lines that are not valid JSON documents. */
        MALFORMED_PLAYER_JSON,

        /** Telemetry lines read by the activity mapper. */
        TELEMETRY_LINES,

        /** Telemetry lines rejected by the shared parser. */
        TELEMETRY_INVALID,

        /** Session-start events without a usable player identifier. */
        TELEMETRY_WITHOUT_PLAYER_ID,

        /** Unique player profiles successfully emitted with activity facts. */
        JOINED_PLAYERS,

        /** Activity groups for which no player profile was found. */
        ACTIVITY_WITHOUT_PLAYER_RECORD,

        /** Repeated player profiles for one identifier, which make the join ambiguous. */
        DUPLICATE_PLAYER_RECORDS
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Emits player records under a profile-first secondary-sort key.
     */
    public static final class PlayerMapper extends Mapper<LongWritable, Text, Text, Text> {

        /** Reusable Hadoop output key for the composite player identifier. */
        private final Text outputKey = new Text();

        /** Reusable Hadoop output value containing the player profile data. */
        private final Text outputValue = new Text();

        /**
         * Projects one JSONL player document to the profile side of the composite join.
         *
         * @param offset the input line offset
         * @param line the raw JSONL player document
         * @param context the mapper context used to write records and quality counters
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

            /*
             * Count profiles without a usable creation timestamp.
             * Keep the profile record so the reducer can still identify the player.
             */
            if(player.createdAt() == null) {
                context.getCounter(QualityCounters.PLAYER_RECORDS_WITHOUT_INSTALL_DATE).increment(1);
            }

            // Build a profile-first composite key for secondary sorting.
            outputKey.set(PlayerJoinKey.composite(player.playerId(), PlayerJoinKey.PROFILE_TAG));

            // Prefix the value with P to identify it as a player profile record.
            outputValue.set("P\t" + installDate(player));

            // Send the player profile to the reducer.
            context.write(outputKey, outputValue);
        }

        /**
         * Converts the creation instant to a calendar date in UTC for cohort assignment.
         *
         * @param player the parsed player document
         * @return the UTC installation date, or an empty string when unavailable
         */
        private String installDate(PlayerDocument player) {

            // Convert the creation instant to a UTC calendar date when available.
            return player.createdAt() == null ? "" : player.createdAt().atOffset(ZoneOffset.UTC).toLocalDate().toString();
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Emits one activity record for each valid session start with a player identifier.
     */
    public static final class ActivityMapper extends Mapper<LongWritable, Text, Text, Text> {

        /** Reusable Hadoop output key for the composite player identifier. */
        private final Text outputKey = new Text();

        /** Reusable Hadoop output value containing the activity date. */
        private final Text outputValue = new Text();

        /**
         * Emits valid session-start activity dates on the event side of the composite join.
         *
         * @param offset the input line offset
         * @param line the raw telemetry JSONL record
         * @param context the mapper context used to write records and quality counters
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

            // Ignore events that do not represent a session start.
            if(!"session_start".equals(event.eventType())) return;

            /*
             * Reject session-start events without a usable player identifier.
             */
            if(event.playerId().isBlank()) {
                context.getCounter(QualityCounters.TELEMETRY_WITHOUT_PLAYER_ID).increment(1);
                return;
            }

            // Convert the event timestamp to a UTC calendar date.
            LocalDate activityDate = event.timestamp().atOffset(ZoneOffset.UTC).toLocalDate();

            // Build an event-side composite key for the player.
            outputKey.set(PlayerJoinKey.composite(event.playerId(), PlayerJoinKey.EVENT_TAG));

            // Prefix the value with A to identify it as an activity record.
            outputValue.set("A\t" + activityDate);

            // Send the activity record to the reducer.
            context.write(outputKey, outputValue);
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Produces one player-level row containing the installation cohort and unique activity dates.
     */
    public static final class CohortJoinReducer extends Reducer<Text, Text, Text, Text> {

        /** Reusable Hadoop output key containing the installation date. */
        private final Text outputKey = new Text();

        /** Reusable Hadoop output value containing the cohort and activity dates. */
        private final Text outputValue = new Text();

        /**
         * Reads the profile-first grouped values and emits one player-level retention fact.
         *
         * @param composite the composite key identifying the player
         * @param values the secondary-sorted profile and activity records for that player
         * @param context the reducer context used to write results and quality counters
         * @throws IOException if writing the retention fact fails
         * @throws InterruptedException if the reducer is interrupted
         */
        @Override
        protected void reduce(Text composite, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

                // Initialize the state used to collect one player's profile and activity dates.
                JoinState state = new JoinState();

                // Process the secondary-sorted values in profile-first order.
                for(Text value : values) {
                    state.accept(value.toString(), context);
                }

                // Count activity groups that have no corresponding player profile.
                if(!state.playerSeen) {
                    context.getCounter(QualityCounters.ACTIVITY_WITHOUT_PLAYER_RECORD).increment(1);
                    return;
                }

                // Skip ambiguous joins when multiple profiles exist for the same player.
                if(state.duplicatePlayer) return;

                // Write the player's installation date, cohort date, and unique activity dates.
                writeFact(state, context);

                // Count the successfully joined player.
                context.getCounter(QualityCounters.JOINED_PLAYERS).increment(1);
            }

        /**
         * Serializes one player's installation date, Monday cohort, and unique activity dates.
         *
         * @param state the collected profile and activity data for the player
         * @param context the reducer context used to write the output record
         * @throws IOException if writing the output record fails
         * @throws InterruptedException if the reducer is interrupted
         */
        private void writeFact(JoinState state, Context context) throws IOException, InterruptedException {

            /*
             * Calculate the cohort date by moving the installation date to the
             * previous Monday or keeping it unchanged when it is already Monday.
             */
            String cohort = state.installDate == null ? "" : state.installDate
                .with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
                .toString();

            // Serialize the sorted, unique activity dates as a comma-separated string.
            String activeDates = state.activityDates.stream()
                .map(LocalDate::toString)
                .collect(Collectors.joining(","));

            // Use the installation date as the output key, or an empty string when unavailable.
            outputKey.set(state.installDate == null ? "" : state.installDate.toString());

            // Store the cohort date and activity dates as tab-separated output fields.
            outputValue.set(cohort + "\t" + activeDates);

            // Emit the player-level retention fact.
            context.write(outputKey, outputValue);
        }

        /**
         * Maintains the bounded profile fields and distinct activity dates for one player key.
         */
        private final class JoinState {

            /** Installation date parsed from the player's profile, when available. */
            private LocalDate installDate;

            /** Indicates whether a player profile has already been processed. */
            private boolean playerSeen;

            /** Indicates that multiple profiles were found for the same player identifier. */
            private boolean duplicatePlayer;

            /** Sorted collection of distinct UTC activity dates for the player. */
            private final TreeSet<LocalDate> activityDates = new TreeSet<>();

            /**
             * Dispatches a tagged value to profile or activity handling.
             *
             * @param rawValue the tagged, tab-separated value emitted by a mapper
             * @param context the reducer context used to increment quality counters
             */
            private void accept(String rawValue, Context context) {

                // Split the tagged value while preserving empty trailing fields.
                String[] fields = rawValue.split("\t", -1);

                // Ignore values that do not contain both a record type and a data field.
                if(fields.length < 2) return;

                // Dispatch the value according to its record type.
                if("P".equals(fields[0])) acceptProfile(fields[1], context);

                // Parse the activity date and add it to the sorted set.
                else if("A".equals(fields[0])) activityDates.add(LocalDate.parse(fields[1]));
            }

            /**
             * Stores the first profile date and flags duplicate profiles as ambiguous.
             *
             * @param date the installation date emitted by the player mapper
             * @param context the reducer context used to increment quality counters
             */
            private void acceptProfile(String date, Context context) {

                /*
                 * Detect duplicate profiles because they make the player join ambiguous.
                 */
                if(playerSeen) {
                    context.getCounter(QualityCounters.DUPLICATE_PLAYER_RECORDS).increment(1);
                    duplicatePlayer = true;
                    return;
                }

                // Mark the player profile as processed.
                playerSeen = true;

                // Parse the installation date when the profile contains a valid date value.
                if(!date.isBlank()) installDate = LocalDate.parse(date);
            }
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Command-line entry point.
     *
     * <p>Usage: {@code <players-jsonl> <telemetry-jsonl> <player-activity-output>}</p>
     *
     * @param args player JSONL path, telemetry JSONL path, and new output path
     * @throws Exception if Hadoop job setup or execution fails
     */
    public static void main(String[] args) throws Exception {

        /*
         * Validate the number of command-line arguments.
         */
        if(args.length != 3) {
            System.err.println(
                "Usage: PlayerCohortJoinJob <players-jsonl> <telemetry-jsonl> <player-activity-output>"
            );
            System.exit(2);
        }

        /* Create the Hadoop job with its descriptive name. */
        Job job = Job.getInstance(new Configuration(), "nexus-player-cohort-activity-join");

        /* Configure the job class and secondary-sort behavior for the player join. */
        job.setJarByClass(PlayerCohortJoinJob.class);
        job.setPartitionerClass(PlayerJoinKey.PartitionerByPlayer.class);
        job.setGroupingComparatorClass(PlayerJoinKey.GroupingComparator.class);

        /* Configure the mapper output key and value types. */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        /* Configure the reducer and final output types. */
        job.setReducerClass(CohortJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /*
         * Register both input sources with their respective mappers.
         * Player profiles and telemetry events are combined by the reducer.
         */
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PlayerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ActivityMapper.class);

        // Configure the destination for the joined player-level records.
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Return a process exit code based on the job execution result.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
