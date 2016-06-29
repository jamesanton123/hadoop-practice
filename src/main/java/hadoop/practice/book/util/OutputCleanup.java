package hadoop.practice.book.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class OutputCleanup {
	private static final String[] HDFS_PATHS = {
			"/tmp/hadoop/randImageOut", 
			"/tmp/hadoop/randomImages"
			};

	public static void main(String[] args) {
		try {
			UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hdfs");
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					try {
						clean();
					} catch (IOException e) {
						e.printStackTrace();
					}
					return null;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void clean() throws FileNotFoundException, IOException{
		FileSystem fs = FileSystemUtil.getFileSystemObject();
		for(String p: HDFS_PATHS){
			Path filePath = new Path(p);
			if (fs.exists(filePath)) {
				fs.delete(filePath, true);
			}
		}
		
	}

}
