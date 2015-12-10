package pig.user.udf;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ExceptionUDF extends EvalFunc<String> {

    public static final String MITCHELL_EXCEPTION = "MitchellException";
    public static final String SEVERE = "SEVERE";
    public static final String ERROR_DATE_TIME = "ErrorDateTime";
    public static final String CLASS_NAME = "ClassName";
    public static final String METHOD_NAME = "MethodName";
    public static final String TYPE = "Type";
    public static final String DESCRIPTION = "Description";
    public static final String SEVER_NAME = "SeverName";

    @Override
    public String exec(Tuple input) throws IOException {

        if (!(input != null && input.size() > 0)) {
            return null;
        }

        String line = (String) input.get(0);
        return parse(line);
    }

    String parse(String line) {

        String returnVal = null;

        if (line != null && line.length() > 0 && line.contains(SEVERE)) {
            if (line.contains(MITCHELL_EXCEPTION)) {
                returnVal = handleMitchellException(line);
            } else if (line.contains("exception") || line.contains("Exception")) {
                returnVal = handleGeneralException(line);
            }
        }
        return returnVal;
    }

    private String handleGeneralException(String line) {

        String errorDateTime = null;
        String className = null;
        String methodName = null;
        String type = null;
        String description = null;
        String serverName = null;
        String exception = null;

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
                description = descAndType.substring(0,
                        descAndType.indexOf("Type")).trim();
                type = descAndType.substring(
                        descAndType.indexOf("Type") + 5).trim();
            }
        }
        return writeOutputResult(exception, errorDateTime, className,
                methodName, type, description, serverName);
    }

    private String handleMitchellException(String line) {

        String errorDateTime = null;
        String className = null;
        String methodName = null;
        String type = null;
        String description = null;
        String serverName = null;

        int pos = line.indexOf(MITCHELL_EXCEPTION);
        String partOfLine = line.substring(pos);
        String[] tokens = partOfLine.split(",");

        if (tokens.length > 1) {

            for (String token : tokens) {
                if (token.contains(TYPE)) {
                    type = token.split(":")[1].trim();
                }
                if (token.contains(ERROR_DATE_TIME)) {
                    errorDateTime = token.substring(15, tokens[1].length())
                            .trim();
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
        return writeOutputResult(MITCHELL_EXCEPTION,errorDateTime, className,
                methodName, type, description, serverName);

    }

    private String writeOutputResult(String exception, String errorDateTime,
                                     String className, String methodName, String type,
                                     String description, String serverName) {

        String returnVal = null;

        if (type != null && errorDateTime != null && className != null
                && methodName != null && description != null
                && serverName != null) {

            returnVal = exception + ", " + type + ", "
                    + errorDateTime + ", " + className + ", " + methodName
                    + ", " + description + ", " + serverName;
        }
        return returnVal;
    }
}