import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_1 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, LongWritable>{

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String flag = context.getConfiguration().get("flag");
            String []row = value.toString().split(",");

            if (row[0].trim().toLowerCase().equals("date")) return; //header

            if (!flag.toLowerCase().trim().equals("true")) {
                if (row[1].trim().toLowerCase().equals("world")) return;
                if (row[1].trim().toLowerCase().equals("international")) return;
            }

            if (row[0].contains("2019-")) return;

            word.set(row[1].trim().toLowerCase());
            try {
                context.write(word, new LongWritable(Long.parseLong(row[2])));
            } catch (Exception e) {//converting string to number
                return;
            }
        }
    }

    public static class LongSumReducer
            extends Reducer<Text,LongWritable,Text,LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("flag", args[1]); // if not "true" exactly, then take it as false
        Job job = Job.getInstance(conf, "Covid19_1");
        job.setJarByClass(Covid19_1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

