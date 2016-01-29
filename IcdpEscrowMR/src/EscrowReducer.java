import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class EscrowReducer extends Reducer<Text, LongWritable, Text, NullWritable> {
    public EscrowReducer() {
    }

    public void reduce(Text var1, Iterable<LongWritable> var2, Context var3) throws IOException, InterruptedException {
        long var4 = 0L;
        boolean var6 = false;
        boolean var7 = false;
        if(var1.toString().contains("\t")) {
            String[] var8 = var1.toString().split("\t");
            if(var8[0].contains("FAILSUCCESS")) {
                Iterator var9 = var2.iterator();

                while(var9.hasNext()) {
                    LongWritable var10 = (LongWritable)var9.next();
                    if(var10.get() == 1L) {
                        var6 = true;
                    } else if(var10.get() == 2L) {
                        var7 = true;
                    }
                }

                if(var6 && var7) {
                    var3.getCounter("COUNT", var8[0]).increment(1L);
                }
            } else {
                var3.getCounter("COUNT", var8[0]).increment(1L);
            }
        } else {
            LongWritable var12;
            for(Iterator var11 = var2.iterator(); var11.hasNext(); var4 += var12.get()) {
                var12 = (LongWritable)var11.next();
            }

            var3.getCounter("COUNT", var1.toString()).increment(var4);
        }

    }
}

