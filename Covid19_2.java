import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_2 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, LongWritable>{

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String []row = value.toString().split(",");

            if (row[0] == null || row[0].trim().toLowerCase().equals("date")) return; //header

            try {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

                Date data_date = format.parse(row[0].trim());
                Date input_start_date = format.parse(context.getConfiguration().get("input_start_date"));
                Date input_end_date = format.parse(context.getConfiguration().get("input_end_date"));

                if (data_date.before(input_start_date) || data_date.after(input_end_date)) return; //filter on date

                word.set(row[1].trim().toLowerCase());
                context.write(word, new LongWritable(Long.parseLong(row[3])));
            } catch (Exception e) {
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
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setLenient(false);
        try {
            Date data_start_date = format.parse("2019-12-31");
            Date data_end_date = format.parse("2020-04-08");
            Date input_start_date = format.parse(args[1]);
            Date input_end_date = format.parse(args[2]);

            if (input_start_date.after(input_end_date) ||
                    (input_start_date.before(data_start_date)) ||
                    (input_end_date.after(data_end_date))
            ) {
                System.out.println("\nInvalid start and end dates, not in range\n");
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("\nInvalid start and end dates\n");
            System.exit(1);
        }

        conf.set("input_start_date", args[1]);
        conf.set("input_end_date", args[2]);

        Job job = Job.getInstance(conf, "Covid19_2");
        job.setJarByClass(Covid19_2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

