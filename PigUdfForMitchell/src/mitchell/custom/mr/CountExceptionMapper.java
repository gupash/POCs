package mitchell.custom.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountExceptionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        IntWritable count = new IntWritable(1);

        String line = value.toString().trim();

        if (line != null) {

            String[] words = line.split("\\t");
            context.write(new Text(words[0]), count);
        }
    }
}
