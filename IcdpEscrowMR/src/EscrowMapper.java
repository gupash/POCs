import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EscrowMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    public EscrowMapper() {
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Entity entity = new Entity(value.toString(), context);
        if (entity.isValid) {
            this.processOS(entity, context);
            this.processDevices(entity, context);
            this.processTOT(entity, context);
        }

    }

    private void processOS(Entity entity, Context context) throws IOException, InterruptedException {
        if (entity.os_valid) {
            context.write(new Text("ICDP." + entity.DF_NAME_SUFFIX + "OS." + entity.os_type + ".UU" + "\t" + entity.prs_id), new LongWritable(0L));
            context.write(new Text("ICDP." + entity.DF_NAME_SUFFIX + "OS." + entity.os_type + "." + entity.os_major_version + ".UU" + "\t" + entity.prs_id), new LongWritable(0L));
            context.write(new Text("ICDP." + entity.DF_NAME_SUFFIX + "OS." + entity.os_type + "." + entity.os_major_version + entity.os_minor_version + ".UU" + "\t" + entity.prs_id), new LongWritable(0L));
        }

    }

    private void processDevices(Entity entity, Context context) throws IOException, InterruptedException {
        if (entity.device_valid) {
            context.write(new Text("ICDP." + entity.DF_NAME_SUFFIX + "DVC." + entity.platform_type + ".UU" + "\t" + entity.prs_id), new LongWritable(0L));
            context.write(new Text("ICDP." + entity.DF_NAME_SUFFIX + "DVC." + entity.platform_name + ".UU" + "\t" + entity.prs_id), new LongWritable(0L));
            if (!entity.platform.contains("MAC")) {
                context.write(new Text("ICDP." + entity.DF_NAME_SUFFIX + "DVC." + entity.platform_name + "." + entity.platform_version + ".UU" + "\t" + entity.prs_id), new LongWritable(0L));
            }
        }

    }

    private void processTOT(Entity entity, Context context) throws IOException, InterruptedException {
        if (entity.command != null && entity.command.contains("ENROLL")) {
            context.write(new Text("ICDP.ENROLL.UU\t" + entity.prs_id), new LongWritable(0L));
        }

        if (entity.command != null && entity.command.contains("ENROLL") && entity.response != null && entity.response.contains("200")) {
            context.write(new Text("ICDP.ENROLL.SUCCESS.UU\t" + entity.prs_id), new LongWritable(0L));
            context.write(new Text("ICDP.ENROLL.RECORD.SUCCESS.UU\t" + entity.label + "\t" + entity.prs_id), new LongWritable(0L));
        }

        if (entity.command != null && entity.command.contains("ENROLL") && entity.response != null && !entity.response.contains("200")) {
            context.write(new Text("ICDP.ENROLL.FAILURE.UU\t" + entity.prs_id), new LongWritable(0L));
        }

        if (entity.command != null && entity.command.contains("RECOVER") && entity.response != null && entity.response.contains("200")) {
            context.write(new Text("ICDP.RECOVER.SUCCESS.UU\t" + entity.prs_id), new LongWritable(0L));
            context.write(new Text("ICDP.RECOVER.RECORD.SUCCESS.UU\t" + entity.label + "\t" + entity.prs_id), new LongWritable(0L));
            context.write(new Text("ICDP.RECOVER.SUCCESS.CNT"), new LongWritable(entity.aggrCt));
            context.write(new Text("ICDP.RECOVER.FAILSUCCESS.UU\t" + entity.prs_id), new LongWritable(1L));
        }

        if (entity.command != null && entity.command.contains("RECOVER") && entity.response != null && !entity.response.contains("200")) {
            context.write(new Text("ICDP.RECOVER.FAILURE.UU\t" + entity.prs_id), new LongWritable(0L));
            context.write(new Text("ICDP.RECOVER.RECORD.FAILURE.UU\t" + entity.label + "\t" + entity.prs_id), new LongWritable(0L));
            context.write(new Text("ICDP.RECOVER.FAILURE.CNT"), new LongWritable(entity.aggrCt));
            context.write(new Text("ICDP.RECOVER.FAILSUCCESS.UU\t" + entity.prs_id), new LongWritable(2L));
        }

        if (entity.command != null && entity.command.contains("RECOVER")) {
            context.write(new Text("ICDP.RECOVER.UU\t" + entity.prs_id), new LongWritable(0L));
            context.write(new Text("ICDP.RECOVER.RECORD.CNT\t" + entity.label + "\t" + entity.prs_id), new LongWritable(0L));
            context.write(new Text("ICDP.RECOVER.CNT"), new LongWritable(entity.aggrCt));
        }

        if (entity.command != null && entity.command.contains("RECOVER") && entity.response != null && !entity.response.contains("200") && entity.errorCd.contains("-6015")) {
            context.write(new Text("ICDP.RECOVER.PCFAILURE.CNT"), new LongWritable(entity.aggrCt));
            context.write(new Text("ICDP.RECOVER.PCFAILURE.UU\t" + entity.prs_id), new LongWritable(entity.aggrCt));
        }

    }
}
