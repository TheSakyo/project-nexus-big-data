package fr.thesakyo.nexus.mapreduce.common;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

/**
 * Shared secondary-sort key for reduce-side joins on player ID.
 *
 * <p>The secondary tag sorts profile records before event records, while the grouping
 * comparator groups both tags under the same player. This lets reducers read a profile
 * once and then stream that player's events without loading an entire source into memory.</p>
 */
public final class PlayerJoinKey {

    /** Delimits a player identifier from its profile/event sort tag. */
    public static final String SEPARATOR = "\u0001";

    /** Sort tag assigned to profile records so they precede that player's event records. */
    public static final String PROFILE_TAG = "0";

    /** Sort tag assigned to event records so reducers can process them after the profile. */
    public static final String EVENT_TAG = "1";

    /** Prevents instantiation of this static join-key utility. */
    private PlayerJoinKey() {}

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Combines a player identifier with a secondary-sort tag.
     *
     * @param playerId stable identifier shared by all records in one join group
     * @param tag sort suffix; profile tags must sort before event tags
     * @return a key understood by this class's partitioner and grouping comparator
     */
    public static String composite(String playerId, String tag) { return playerId + SEPARATOR + tag; }

    /**
     * Extracts the player identifier from a composite Hadoop text key.
     *
     * @param compositeKey key in {@code playerId + separator + tag} form
     * @return the identifier, or the original text if no separator is present
     */
    public static String playerId(Text compositeKey) {

        // Convert the Hadoop text key into a standard Java string.
        String value = compositeKey.toString();

        // Find the separator between the player identifier and the sort tag.
        int separator = value.indexOf(SEPARATOR);

        // Return only the player identifier when a separator exists.
        return separator < 0 ? value : value.substring(0, separator);
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Partitions all records for one player to the same reducer.
     */
    public static final class PartitionerByPlayer extends Partitioner<Text, Text> {

        /**
         * Assigns every profile and event key for a player to one reducer partition.
         *
         * @param key composite player and sort-tag key
         * @param value associated shuffle value (not used for partition selection)
         * @param partitionCount number of reducer partitions configured by Hadoop
         * @return a partition index shared by all records belonging to this player
         */
        @Override
        public int getPartition(Text key, Text value, int partitionCount) {

            /*
             * Extract the player identifier and calculate a non-negative hash.
             * Use the hash to ensure all records for the same player reach the same reducer.
             */
            return (playerId(key).hashCode() & Integer.MAX_VALUE) % partitionCount;
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

    /**
     * Groups profile and event keys by player; Hadoop's full-key sort keeps profiles first.
     */
    public static final class GroupingComparator extends WritableComparator {

        /**
         * Registers the comparator for Hadoop {@link Text} keys.
         */
        public GroupingComparator() { super(Text.class, true); }

        /**
         * Compares already-deserialized keys using only their player identifiers.
         *
         * @param left the first composite player join key
         * @param right the second composite player join key
         * @return a negative value, zero, or a positive value according to the player identifiers
         */
        @Override
        public int compare(WritableComparable left, WritableComparable right) {

            // Compare the player identifiers while ignoring the profile/event tag.
            return playerId((Text) left).compareTo(playerId((Text) right));
        }

        /**
         * Compares Hadoop's serialized {@link Text} keys without relying on deserialized key objects.
         *
         * @param left the serialized bytes of the first key
         * @param leftStart the starting offset of the first key
         * @param leftLength the length of the first serialized key
         * @param right the serialized bytes of the second key
         * @param rightStart the starting offset of the second key
         * @param rightLength the length of the second serialized key
         * @return a negative value, zero, or a positive value according to the player identifiers
         */
        @Override
        public int compare(byte[] left, int leftStart, int leftLength, byte[] right, int rightStart, int rightLength) {

            /*
             * Decode both keys and compare only their player identifiers.
             */
            return playerId(decode(left, leftStart, leftLength)).compareTo(
                playerId(decode(right, rightStart, rightLength))
            );
        }

        /**
         * Decodes one Hadoop {@link Text} key from the sort buffer.
         *
         * @param bytes the serialized key buffer
         * @param start the starting offset of the key
         * @param length the length of the serialized key
         * @return the decoded Hadoop text key
         * @throws IllegalArgumentException if the serialized key cannot be decoded
         */
        private static Text decode(byte[] bytes, int start, int length) {

            /*
             * Create an input buffer over the serialized key bytes.
             */
            DataInputBuffer input = new DataInputBuffer();
            input.reset(bytes, start, length);

            /*
             * Deserialize the key into a Hadoop Text instance.
             */
            Text text = new Text();
            try {
                text.readFields(input);
                return text;
            } catch(IOException exception) {
                throw new IllegalArgumentException("Invalid serialized player join key", exception);
            }
        }
    }
}
