package hadoop.practice.book.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class CommandExecutor {
	private static final String SCP_JAR_TO_HADOOP_JOBS = "pscp \"C:\\dev-workspace\\hadoop-practice\\target\\hadoop-practice-0.0.1-SNAPSHOT-job.jar\" root@192.168.126.128:/hadoop-jobs/wordcount.jar";
	private static final String SCP_JAR_TO_JAMES_FOLDER = "pscp \"C:\\Users\\James\\Desktop\\i.jar\" root@192.168.81.128:/james/i.jar" ;
	
	
	public static void main(String[] args) {
		String resp = CommandExecutor.executeCommand(SCP_JAR_TO_JAMES_FOLDER);
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
