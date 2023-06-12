package io.github.seabow.datax.common;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;

public class ShellUtils {
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

    public static void main(String[] args) throws Exception {
        System.out.println(runShellForStringSubstitution("date -v-1d +%Y-%m-%d"));
    }
}
