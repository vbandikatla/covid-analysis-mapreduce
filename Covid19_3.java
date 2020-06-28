import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_3 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String []row = value.toString().split(",");

            if (row[0] == null || row[0].trim().toLowerCase().equals("date")) return; //header

            word.set(row[1].trim().toLowerCase());
            try {
                context.write(word, new DoubleWritable(Double.parseDouble(row[2])));
            } catch (Exception e) {
                return;
            }
        }
    }

    public static class DoubleSumReducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        private HashMap<String, Double> population = new HashMap<String, Double>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try{
                URI[] files = context.getCacheFiles();
                for (URI file : files){
                    readFile(file, context);
                }
            } catch(IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
            }
        }

        private void readFile(URI file, Context context) {
            try{
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(file.toString());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
                String line = null;
                while((line = bufferedReader.readLine()) != null) {
                    String []row = line.split(",");
                    if (row == null || row[1].trim().toLowerCase().equals("location")) continue; //location or null values
                    try {
                        double cnt = Double.parseDouble(row[4]);
                        if (row[0] == null || cnt == 0 || population.containsKey(row[0].trim().toLowerCase())) continue;
                        population.put(row[0].trim().toLowerCase(), cnt);
                        if (row[1] == null || cnt == 0 || population.containsKey(row[1].trim().toLowerCase())) continue;
                        population.put(row[1].trim().toLowerCase(), cnt);
                    } catch(Exception e) {
                        System.err.println("Adding population to map: " + e.getMessage());
                    }
                }
            } catch(IOException ex) {
                System.err.println("Exception while reading population file: " + ex.getMessage());
            }
        }

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            if (!population.containsKey(key.toString().trim())) return;
            double sum = 0L;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            sum = sum * 1000000L;
            sum /= population.get(key.toString().trim());
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Covid19_3");
        job.addCacheFile(new Path(args[1]).toUri());
        job.setJarByClass(Covid19_3.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(DoubleSumReducer.class);
        job.setReducerClass(DoubleSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

