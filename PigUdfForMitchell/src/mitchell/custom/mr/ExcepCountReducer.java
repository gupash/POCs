package mitchell.custom.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class ExcepCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    int sum;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int specificExpCount = 0;

        for (IntWritable value : values) {
            specificExpCount += value.get();
        }
        sum += specificExpCount;
        context.write(key, new IntWritable(specificExpCount));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        context.write(new Text("Total"), new IntWritable(sum));
    }
}
