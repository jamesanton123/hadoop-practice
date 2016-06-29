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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.security.UserGroupInformation;

public class Seeder {

	public static void main(String[] args) {
		try {
			UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hdfs");
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					try {
//						createImages(5000);
						copyToHdfs(5000);
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

	private static void createImages(int numImages){
		for (int i = 0; i < numImages; i++) {
			RandomImageGenerator.generateImage("C:\\users\\james\\Desktop\\randomImages\\" + i + ".png", 4, 4);
			System.out.println(i);
		}
	}
	
	private static void copyToHdfs(int numImages) throws IOException {
		

		Configuration conf = FileSystemUtil.getConfigurationObject();
		FileContext fc = FileContext.getFileContext(conf);

		int numImagesPerSequence = 1000;
		int numSequenceFiles = 5;
		Text key = new Text();
		BytesWritable value = new BytesWritable();
		String folderPath = "/tmp/hadoop/";
		FileSystemUtil.getFileSystemObject().mkdirs(new Path(folderPath));

		// Create the folder to hold the images
		String folderPathRoot = "/tmp/hadoop/randomImages/";
		FileSystemUtil.getFileSystemObject().mkdirs(new Path(folderPathRoot));
		
		String localPathToImage = "C:\\users\\James\\Desktop\\randomImages\\";
		int count = 0;
		for (int sequenceNumber = 1; sequenceNumber <= numSequenceFiles; sequenceNumber++) {
			System.out.println("Creating Sequence File # " + sequenceNumber);
			String sequenceFilePath = folderPathRoot + "/sequenceFileNum" + sequenceNumber + ".seq";
			Path path = new Path(sequenceFilePath);
			// Setup sequence file
			Writer w = SequenceFile.createWriter(fc, conf, path, key.getClass(), value.getClass(), CompressionType.NONE,
					null, new Metadata(), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
					CreateOpts.bufferSize(4048));
			for (int imageInSequence = 1; imageInSequence <= numImagesPerSequence; imageInSequence++) {
				System.out.println("Writing image # " + count + " to sequence " + sequenceNumber);
				BufferedImage image = ImageIO.read(new File(localPathToImage + count + ".png"));
				ByteArrayOutputStream b = new ByteArrayOutputStream();
				ImageIO.write(image, "png", b);
	            byte[] bytes = b.toByteArray();
	            key.set(String.valueOf(count) + ".png");
				value.set(bytes, 0, bytes.length);
				w.append(key, value);
				count++;
			}
			IOUtils.closeStream(w);
		}
	}
}
