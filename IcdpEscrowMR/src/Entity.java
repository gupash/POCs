import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Entity {
    public String prs_id;
    public String label;
    public String event_type;
    public String command;
    public String response;
    public String user_agent;
    public String clubKey;
    public String clubKeyFull;
    public String platform;
    public String platform_type;
    public String platform_version;
    public String platform_name;
    public boolean device_valid = false;
    public String os_value;
    public String os_name;
    public String os_type;
    public String os_ver;
    public String os_major_version;
    public String os_minor_version;
    public long aggrCt;
    public String errorCd;
    public boolean os_valid = false;
    public String DF_NAME_SUFFIX = "INVALID.";
    public boolean isValid;

    public Entity(String var1, Context var2) throws IOException {

        String[] var3 = var1.split("\t");

        if (var3.length < 10) {
            this.isValid = false;
        } else {
            this.prs_id = var3[0];
            this.label = var3[1];
            this.event_type = var3[3];
            this.command = var3[5];
            this.user_agent = var3[6];
            this.response = var3[4];
            String[] var4 = (new UAParser()).parseUserAgent(this.user_agent).split("\t");
            this.platform = var4[0];
            this.os_value = var4[1];
            this.errorCd = var3[8];
            if (var3.length > 11 && var3[11].matches("[0-9]+")) {
                this.aggrCt = Long.parseLong(var3[11]);
            }

            if (this.label != null && this.label.contains("com.apple.icdp.record") && this.response != null && !this.response.equals("") && (this.command != null && this.command.contains("ENROLL") || this.command != null && this.command.contains("RECOVER"))) {
                this.isValid = true;
            }

            if (this.label != null && this.label.contains("com.apple.icdp.record") && (this.command != null && this.command.contains("ENROLL") && this.response != null && !this.response.equals("") && this.response.contains("200") || this.command != null && this.command.contains("RECOVER") && this.response != null && !this.response.equals("") && this.response.contains("200"))) {
                this.os_valid = true;
                this.device_valid = true;
                this.DF_NAME_SUFFIX = this.command.equalsIgnoreCase("ENROLL") ? "ENROLL." : "RECOVER.";
                if (var4[1] != null && (var4[1].contains(".") || var4[1].contains(" "))) {
                    this.os_value = this.os_value.replace(".", "_");
                    this.os_value = this.os_value.replaceAll(" ", "_");
                }

                if (this.os_value != null && !this.os_value.equalsIgnoreCase("UNKNOWN") && !this.os_value.equalsIgnoreCase("UNKNOWNUA")) {
                    this.os_valid = true;
                    String[] var5 = this.os_value.split(";");
                    this.os_name = var5[0];
                    this.os_ver = var5[1];
                    this.os_major_version = this.os_ver.substring(0, this.os_ver.indexOf("_"));
                    this.os_minor_version = this.os_ver.substring(this.os_ver.indexOf("_"));
                    this.os_type = this.os_name.contains("Mac") ? "OSX" : (this.os_name.contains("iPhone") ? "IOS" : (this.os_name.contains("Apple") ? "TVOS" : (this.os_name.contains("Watch") ? "WATCHOS" : "UNKONWN")));
                    if (this.os_value.contains("Mac")) {
                        if (this.os_ver.split("_").length == 2) {
                            this.os_ver = this.os_ver + "_0";
                        }

                        this.os_major_version = this.os_ver.substring(0, this.os_ver.indexOf("_", 3));
                        this.os_minor_version = this.os_ver.substring(this.os_ver.indexOf("_", 3));
                    } else if (this.os_ver.split("_").length == 1) {
                        this.os_value = this.os_value + "_0";
                    }
                }

                this.platform = this.platform.replaceAll(",", "_");
                if (this.platform != null && !this.platform.equalsIgnoreCase("UNKNOWN") && !this.platform.equalsIgnoreCase("UNKNOWNUA")) {
                    this.platform = this.platform.toUpperCase();
                    Matcher var7 = Pattern.compile("\\d+").matcher(this.platform);
                    if (var7.find()) {
                        int var6 = Integer.valueOf(var7.group()).intValue();
                        this.platform_name = this.platform.substring(0, this.platform.indexOf(var6 + ""));
                        this.platform_version = this.platform.substring(this.platform.indexOf(var6 + ""));
                    } else {
                        this.platform_name = this.platform;
                    }

                    this.device_valid = true;
                    if (this.platform.startsWith("IPAD")) {
                        this.platform_type = "IPAD";
                    } else if (this.platform.startsWith("IPOD")) {
                        this.platform_type = "IPOD";
                    } else if (this.platform.startsWith("IPHONE")) {
                        this.platform_type = "IPHONE";
                    } else if (!this.platform.startsWith("MAC") && !this.platform.startsWith("IMAC")) {
                        this.platform_type = "UNKNOWN";
                    } else {
                        this.platform_type = "MAC";
                    }
                }

            }
        }
    }
}
