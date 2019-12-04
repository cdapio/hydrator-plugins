package co.cask.hydrator.common;

import com.google.common.base.Strings;

import java.nio.file.Files;
import java.nio.file.Paths;

public class KeyStoreUtil {

    public static String getValidPath(String keyStorePath) {

        if (!Strings.isNullOrEmpty(keyStorePath)) {

            if (keyStorePath.startsWith("file://")) {
                keyStorePath = keyStorePath.substring(7);
            }

            // In case file is send through --files option in Spark-Submit the file will be available at "./" location
            // Eg : Path : "/var/log/mycert.p12" is available inside Spark as "./mycert.p12"

            // first check is file is available in local classpath (ie passed using --files option)
            String[] tokens = keyStorePath.split("/");
            if (tokens.length > 0) {
                String localPath = "./" + tokens[tokens.length - 1];
                if (Files.exists(Paths.get(localPath))) {
                    return localPath;
                }
            }

            // if file is available at full path on machine
            if (Files.exists(Paths.get(keyStorePath))) {
                return keyStorePath;
            }
        }

        return keyStorePath;
    }
}
