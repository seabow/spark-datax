package io.github.seabow.datax.common;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;

/**
 * The type Shell utils.
 */
public class ShellUtils {
    /**
     * Run shell for string substitution string.
     *
     * @param expr the expr
     * @return the string
     * @throws Exception the exception
     */
    public static String runShellForStringSubstitution(String expr) throws Exception{
        String[] fullCommand = {"sh", "-c", "echo $(" + expr+")"};
        Process process = Runtime.getRuntime().exec(fullCommand);

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream())
        );
        process.waitFor();

        String line;
        StringBuilder stdout = new StringBuilder();
        while (Objects.nonNull(line = reader.readLine())) {
            if (stdout.length() != 0) {
                stdout.append("\n");
            }
            stdout.append(line);
        }
        return stdout.toString();
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        System.out.println(runShellForStringSubstitution("date -v-1d +%Y-%m-%d"));
    }
}
