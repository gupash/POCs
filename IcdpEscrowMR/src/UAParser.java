import java.io.IOException;

public class UAParser {
    public UAParser() {
    }

    public String parseUserAgent(String var1) throws IOException {
        String var2 = null;

        try {
            var2 = this.parseUAString(var1);
        } catch (IOException var4) {
            var2 = "UNKNOWNUA\tUNKNOWNUA\tUNKNOWNUA";
        }

        return var2;
    }

    private String parseUAString(String var1) throws IOException {
        if(var1 != null && var1.length() > 0) {
            if(var1.trim().equals("null")) {
                throw new IOException("Invalid UA string format");
            } else {
                var1 = var1.trim();
                if(var1.length() <= 0) {
                    throw new IOException("UA string contains only blank spaces");
                } else if(var1.charAt(0) == 60 && var1.charAt(var1.length() - 1) == 62) {
                    var1 = var1.substring(1, var1.length() - 1);
                    if(var1.contains("%20")) {
                        var1 = var1.replaceAll("%20", " ");
                    }

                    String[] var2 = null;
                    var2 = var1.split(">\\s*<");
                    if(var2 != null && var2.length == 3) {
                        StringBuffer var3 = new StringBuffer(1000);
                        var3.append(var2[0]);
                        var3.append('\t');
                        var3.append(var2[1]);
                        var3.append('\t');
                        var3.append(var2[2]);
                        return var3.toString();
                    } else {
                        throw new IOException("Invalid UA string format");
                    }
                } else {
                    throw new IOException("Invalid UA string format");
                }
            }
        } else {
            throw new IOException("Invalid UA string format");
        }
    }

    private String getOSDetails(String var1) throws IOException {
        StringBuffer var2 = new StringBuffer(100);
        String[] var3 = var1.split(";");
        if(var3 != null && var3.length == 3) {
            var2.append(var3[0]);
            var2.append(' ');
            var2.append(var3[1]);
            return var2.toString();
        } else {
            throw new IOException("Invalid UA string format");
        }
    }
}
