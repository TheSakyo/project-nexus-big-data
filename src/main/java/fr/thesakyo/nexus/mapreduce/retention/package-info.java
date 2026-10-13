/**
 * MapReduce jobs that join player installation profiles to activity and calculate retention.
 *
 * <p>The join and metric calculation remain separate jobs so their intermediate data can be
 * inspected and each step can be rerun independently.
 */
package fr.thesakyo.nexus.mapreduce.retention;
