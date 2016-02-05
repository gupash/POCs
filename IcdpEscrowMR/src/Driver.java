import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Iterator;

public class Driver extends Configured implements Tool {
    private static final Logger LOGGER = Logger.getLogger(Driver.class);

    public Driver() {
    }

    public static void main(String[] var0) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Driver(), var0));
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        if(args.length < 1) {
            LOGGER.error("This job expects two command line arguments: args[0] is input file path, args[2] is output file path");
            return 1;
        } else {
            LOGGER.info("Job Input1: " + args[0]);
            LOGGER.info("Job Input2:" + args[1]);
            //LOGGER.info("Job Output1:" + var1[2]);
            String least_dim_fact = conf.get("LEAST_DIM_FACT", "0");
            LOGGER.info("Setting least value for Dim Fact =" + least_dim_fact);
            Job job = new Job(conf);
            job.setJarByClass(Driver.class);
            job.setMapperClass(EscrowMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setReducerClass(EscrowReducer.class);
            job.setOutputFormatClass(NullOutputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setNumReduceTasks(100);
            FileInputFormat.addInputPaths(job, args[0]);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            boolean success = job.waitForCompletion(true);
            FileSystem fileSystem = FileSystem.get(conf);
            Path path = new Path(args[1] + "/" + "part-r-00000.txt");
            FSDataOutputStream fsDataOutputStream = fileSystem.create(path, true);
            if(path != null) {
                BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));
                Iterator iterator = ((CounterGroup)job.getCounters().getGroup("COUNT")).iterator();
                int i = 0;

                while(iterator.hasNext()) {
                    Counter next = (Counter)iterator.next();
                    String name = next.getName();
                    if(!name.contains("null") && !name.contains("NULL") && !name.contains("ENROLL")) {
                        try {
                            i = Integer.parseInt(least_dim_fact);
                        } catch (Exception e) {
                            LOGGER.info("Please enter a valid Dim fact limit..");
                        }

                        if(next.getValue() > (long)i) {
                            bufferedWriter.write(name + "\t" + next.getValue());
                        }

                        bufferedWriter.newLine();
                    }
                }

                bufferedWriter.flush();
                bufferedWriter.close();
            }

            return 0;
        }
    }
}

