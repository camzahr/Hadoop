/**
 * Created by jeremycamilleri on 13/10/2016.
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ByGender {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable zero = new IntWritable(0);
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // Separate each information to get the one we want
            String[] data = value.toString().split(";");

            // The gender is in the second part of the data set
            String gender = data[1];

            // Let's determine if the gender corresponds to a male and/or a female
            boolean isMale = gender.equals("m") || gender.equals("m,f") || gender.equals("f,m");
            boolean isFemale = gender.equals("f") || gender.equals("m,f") || gender.equals("f,m");

            // Let's write on both keys (male and female) the "boolean-to-IntWritable conversion" of the booleans
            context.write(new Text("male"), isMale ? one : zero);
            context.write(new Text("female"), isFemale ? one : zero);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,FloatWritable> {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Initialize the final sum and the total number of element with 0
            float sum = 0;
            float total = 0;

            // Let's browse the given values list
            for (IntWritable val : values) {
                // The current value is added to the final sum
                sum += val.get();

                // The total number of elements is incremented
                ++total;
            }

            // The result is calculated as a percentage
            result.set(sum / total * 100);

            // The key is the given word, and the value is the percentage
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "3. Count by Gender");
        job.setJarByClass(ByGender.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}