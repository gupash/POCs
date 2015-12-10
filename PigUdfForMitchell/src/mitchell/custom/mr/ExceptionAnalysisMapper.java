package mitchell.custom.mr;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ExceptionAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

    public static final String SEVERE = "SEVERE";
    public static final String ERROR_DATE_TIME = "ErrorDateTime";
    public static final String CLASS_NAME = "ClassName";
    public static final String METHOD_NAME = "MethodName";
    public static final String TYPE = "Type";
    public static final String DESCRIPTION = "Description";
    public static final String SEVER_NAME = "SeverName";

    private List<String> exceptionCategories = new ArrayList<>();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        URI[] paths = context.getCacheFiles();
        for (URI path : paths) {
            if (FilenameUtils.getName(path.toString()).equals("ExceptionCategories")) {
                loadExceptionCategories(path.getPath());
            }
        }
    }

    private void loadExceptionCategories(String path) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = new FileInputStream(path);
        properties.load(inputStream);
        String exceptionNames = properties.getProperty("ExceptionNames");
        exceptionCategories.addAll(Arrays.asList(exceptionNames.split(",")));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        if (line != null && line.length() > 0 && line.contains(SEVERE)) {
            String exception = getExceptionName(line);

            if (exception != null) {
                handleSpecificException(context, line, exception);
            } else {
                handleGeneralException(context, line);
            }
        }
    }

    private void handleSpecificException(Context context, String line, String exception) throws IOException, InterruptedException {

        String errorDateTime = null;
        String className = null;
        String methodName = null;
        String type = null;
        String description = null;
        String serverName = null;

        int pos = line.indexOf(exception);
        String partOfLine = line.substring(pos);
        String[] tokens = partOfLine.split(",");

        if (tokens.length > 1) {

            for (String token : tokens) {
                if (token.contains(TYPE)) {
                    type = token.split(":")[1].trim();
                }
                if (token.contains(ERROR_DATE_TIME)) {
                    errorDateTime = token.substring(15, tokens[1].length()).trim();
                }
                if (token.contains(CLASS_NAME)) {
                    className = token.split(":")[1].trim();
                }
                if (token.contains(METHOD_NAME)) {
                    methodName = token.split(":")[1].trim();
                }
                if (token.contains(DESCRIPTION)) {
                    description = token.split(":")[1].trim();
                }
                if (token.contains(SEVER_NAME)) {
                    serverName = token.split(":")[1].trim();
                }
            }
        }
        context.write(new Text(exception), new Text(type + ", " + errorDateTime + ", " + className + ", " + methodName + ", " + description + ", " + serverName));
    }

    private void handleGeneralException(Context context, String line) throws IOException, InterruptedException {

        String errorDateTime = null;
        String className = null;
        String methodName = null;
        String type = null;
        String description = null;
        String serverName = null;
        String exception = null;

        if (line.contains("exception") || line.contains("Exception")) {

            String partOfLine = line.split("\\)\\)")[1].trim();
            String[] tokens = partOfLine.split("%%");
            if (tokens.length > 1) {

                if (tokens[7] != null)
                    errorDateTime = tokens[7].trim();
                if (tokens[8] != null)
                    className = tokens[8].trim();
                if (tokens[9] != null)
                    methodName = tokens[9].trim();
                if (tokens[13] != null) {
                    String[] tmp = tokens[13].split("]");
                    serverName = tmp[0].substring(1).trim();
                    exception = tmp[1].split(":")[0].trim();
                    String descAndType = tmp[1].split(":")[1].trim();
                    description = descAndType.substring(0, descAndType.indexOf("Type")).trim();
                    type = descAndType.substring(descAndType.indexOf("Type") + 5).trim();
                }
                context.write(new Text(exception), new Text(type + ", " + errorDateTime + ", " + className + ", " + methodName + ", " + description + ", " + serverName));
            }
        }
    }

    private String getExceptionName(String line) {
        for (String exception : exceptionCategories) {
            if (line.contains(exception)) {
                return exception;
            }
        }
        return null;
    }
}