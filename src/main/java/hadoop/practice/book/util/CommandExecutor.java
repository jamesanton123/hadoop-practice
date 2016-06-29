package hadoop.practice.book.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class CommandExecutor {
	public static void main(String[] args) {
		String resp = CommandExecutor.executeCommand("pscp \"C:\\dev-workspace\\hadoop-practice\\target\\hadoop-practice-0.0.1-SNAPSHOT-job.jar\" root@192.168.126.128:/hadoop-jobs/wordcount.jar");
		System.out.println(resp);
	}

	private static String executeCommand(String command) {

		StringBuffer output = new StringBuffer();

		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return output.toString();

	}
}
