package Common;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Definition of common methods used in Job package
 */
public class Job {

    /**
     * Creates a map that associates input options with its values
     * @param args Input arguments
     * @return Map with options
     */
    public static Map<String, String> getInputOptions(String[] args) {
        Map<String, String> options = new HashMap<>();
        Pattern optionPattern = Pattern.compile("(?<=--)(.*?)(?==)");
        Pattern valuePattern = Pattern.compile("(?<==)(.*)");

        for(String arg: args) {
            Matcher optionMatcher = optionPattern.matcher(arg);
            Matcher valueMatcher = valuePattern.matcher(arg);

            if (optionMatcher.find() && valueMatcher.find()) {
                options.put(optionMatcher.group(), valueMatcher.group());
            }
        }

        return options;
    }

    /**
     * Given a folder and file, returns the path to file
     * @param dir Directory of file
     * @param file File name
     * @return Path to file
     */
    public static String glueDirWithFile(String dir, String file) {
        String glue = dir.matches(".*/$") ? "" : "/";
        return dir + glue + file;
    }

    /**
     * Checks which options are missing
     * @param options Existing options
     * @param required Required options to verify
     * @return List with missing options
     */
    public static List<String> missingOptions(Map<String, String> options, List<String> required) {
        return required.stream().filter(a ->  !options.containsKey(a)).collect(Collectors.toList());
    }

    /**
     * Creates a String indicating which options are missing
     * @param missingOptions Options that are missing
     * @return Generated String referring to missing options
     */
    public static String missingOptionsString(List<String> missingOptions) {
        StringBuilder sb = new StringBuilder();

        for (String option: missingOptions) {
            sb.append("--").append(option).append(" ");
        }

        return sb.toString();
    }
}
