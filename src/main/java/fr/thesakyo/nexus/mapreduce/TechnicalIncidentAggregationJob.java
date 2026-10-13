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

/** Sums per-player joined counts and emits a daily crash/session rate by version and hardware profile. */
public final class TechnicalIncidentAggregationJob {
    private TechnicalIncidentAggregationJob() { }

    public static final class CountMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text dimensionKey = new Text();
        private final Text counts = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\t", -1);
            if (fields.length != 7) {
                context.getCounter("TechnicalIncidentAggregation", "INVALID_JOINED_ROW").increment(1);
                return;
            }
            try {
                Long.parseLong(fields[4]);
                Long.parseLong(fields[5]);
                Long.parseLong(fields[6]);
            } catch (NumberFormatException exception) {
                context.getCounter("TechnicalIncidentAggregation", "INVALID_COUNT").increment(1);
                return;
            }
            dimensionKey.set(String.join("\t", fields[0], fields[1], fields[2], fields[3]));
            counts.set(String.join("\t", fields[4], fields[5], fields[6]));
            context.write(dimensionKey, counts);
        }
    }

    public static final class SumReducer extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        @Override
        protected void reduce(Text dimension, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long sessions = 0;
            long crashes = 0;
            long disconnects = 0;
            for (Text value : values) {
                String[] fields = value.toString().split("\t", -1);
                if (fields.length == 3) {
                    sessions += Long.parseLong(fields[0]);
                    crashes += Long.parseLong(fields[1]);
                    disconnects += Long.parseLong(fields[2]);
                }
            }
            String crashRate = sessions == 0 ? "NA" : Double.toString((double) crashes / sessions);
            result.set(sessions + "\t" + crashes + "\t" + disconnects + "\t" + crashRate);
            context.write(dimension, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TechnicalIncidentAggregationJob <joined-counts-input> <analytics-output>");
            System.exit(2);
        }
        Job job = Job.getInstance(new Configuration(), "nexus-technical-incident-profile-metrics");
        job.setJarByClass(TechnicalIncidentAggregationJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(SumReducer.class);
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
