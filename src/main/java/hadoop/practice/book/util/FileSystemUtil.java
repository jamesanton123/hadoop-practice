package hadoop.practice.book.util;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.CreateOpts.BlockSize;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;

public class FileSystemUtil {

	

	/**
	 * If you get permission denied then run these commands
	 * 
	 *  su - hdfs
	 *	hdfs dfs -chown -R james:hadoop /tmp/hadoop/randomImages
	 * 
	 * 
	 * @param numImages
	 * @throws IOException
	 */
	

	public void copyOneLocalFolderToADestinationFolder(){
//		try {
//		FileUtil.copy(new File("C:\\users\\james\\Desktop\\randomImages"), getFileSystemObject(),
//				new Path("/tmp/hadoop/randomImages"), false, getConfigurationObject());
//	} catch (IllegalArgumentException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	} catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
	}
	
	public static Configuration getConfigurationObject() {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("fs.defaultFS", "hdfs://192.168.32.128:8020/");
		conf.set("hadoop.tmp.dir", "/tmp/hadoop");
		conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", LocalFileSystem.class.getName());
		return conf;
	}

	public static FileSystem getFileSystemObject() {
		System.setProperty("hadoop.home.dir", "/");
		// ApplicationContext context = new
		// ClassPathXmlApplicationContext("spring/hadoop.xml");

		try {
			return FileSystem.get(getConfigurationObject());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
