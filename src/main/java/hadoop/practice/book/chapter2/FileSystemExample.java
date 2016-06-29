package hadoop.practice.book.chapter2;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.io.Text;

import hadoop.practice.book.util.FileSystemUtil;

public class FileSystemExample {
	public static void main(String[] args) throws IOException {
		FileSystemExample trial = new FileSystemExample();
		trial.writeWordCountToHdfs("Hello World Bye World.", "/tmp/hadoop/wordcount/input/file01");
		trial.writeWordCountToHdfs("Hello Hadoop Goodbye Hadoop", "/tmp/hadoop/wordcount/input/file02");

		// trial.writeLanguageFileToHdfs();
		// trial.runTextFileExample();
		// trial.runSequenceFileExample();
		// trial.runMapFileExample();
		// trial.runSetFileExample();
		// trial.runArrayFileExample();
		// trial.runBloomMapFileExample();
		// trial.runSequenceWriterExample();
	}

	private void writeWordCountToHdfs(String content, String path) throws IOException {
		FileSystem fs = FileSystemUtil.getFileSystemObject();
		Path filePath = new Path(path);
		fs.createNewFile(filePath);
		FSDataOutputStream out = fs.create(filePath);
		out.write(content.getBytes());
		out.close();
	}

	private void runSequenceWriterExample() throws IOException {
		String dirName = "/tmp/hadoop/ch2-sequence-writer-example";
		Path p = new Path(dirName);
		Text key = new Text();
		key.set("SomeKey");
		Text value = new Text();
		value.set("SomeValue");
		FileSystem fs = FileSystemUtil.getFileSystemObject();
		Configuration conf = FileSystemUtil.getConfigurationObject();
		SequenceFile.Writer sequenceWriter = new SequenceFile.Writer(fs, conf, p, key.getClass(), value.getClass(),
				fs.getConf().getInt("io.file.buffer.size", 4096), fs.getDefaultReplication(), 1073741824, null,
				new Metadata());
		sequenceWriter.append(key, value);
		IOUtils.closeStream(sequenceWriter);

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
		System.out.println(reader.getKeyClass().getName());

		System.out.println("isCompressed: " + reader.isCompressed());
		System.out.println("isBlockCompressed: " + reader.isBlockCompressed());

		// Create a Text object to store the result of the reads
		Text keyRead = new Text();
		while (reader.next(keyRead)) {
			System.out.println(keyRead);
			Text valueRead = new Text();
			reader.getCurrentValue(valueRead);
			System.out.println(valueRead.toString());
		}
		IOUtils.closeStream(reader);
	}

	/**
	 * The BloomMapFile extends the MapFile adding another file, the bloom file
	 * “/bloom”, and this file contains a serialization of the
	 * DynamicBloomFilter filled with the added keys. The bloom file is written
	 * entirely during the close operation.
	 * 
	 * @throws IOException
	 */
	private void runBloomMapFileExample() throws IOException {
		String dirName = "/tmp/hadoop/ch2-BloomMapFile-example/";
		FileSystem fs = FileSystemUtil.getFileSystemObject();
		Configuration conf = FileSystemUtil.getConfigurationObject();

		// Write a bloommapfile
		Text txtKey = new Text();
		txtKey.set("TestKey");
		Text txtValue = new Text();
		txtValue.set("TestValue");
		// Create an instance of MapFile.Writer and call append(), to add
		// key-values, in order.
		BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, fs, dirName, txtKey.getClass(), txtValue.getClass());
		writer.append(txtKey, txtValue);
		writer.close();

		// Read a bloommapfile
		BloomMapFile.Reader reader = new BloomMapFile.Reader(fs, dirName, conf);
		System.out.println(reader.getKeyClass().getName());

		// Create a Text object to store the result of the reads
		Text txtValue1 = new Text();
		reader.get(txtKey, txtValue1);
		reader.close();
		System.out.println(txtValue1.toString());
	}

	private void runMapFileExample() throws IOException {
		String dirName = "/tmp/hadoop/ch2-MapFile-example/";
		FileSystem fs = FileSystemUtil.getFileSystemObject();
		Configuration conf = FileSystemUtil.getConfigurationObject();

		// Write a mapfile
		Text txtKey = new Text();
		txtKey.set("TestKey");
		Text txtValue = new Text();
		txtValue.set("TestValue");
		// Create an instance of MapFile.Writer and call append(), to add
		// key-values, in order.
		MapFile.Writer writer = new MapFile.Writer(conf, fs, dirName, txtKey.getClass(), txtValue.getClass());
		writer.append(txtKey, txtValue);
		writer.close();

		// Read a mapfile
		MapFile.Reader reader = new MapFile.Reader(fs, dirName, conf);
		System.out.println(reader.getKeyClass().getName());

		// Create a Text object to store the result of the reads
		Text txtValue1 = new Text();
		reader.get(txtKey, txtValue1);
		reader.close();
		System.out.println(txtValue1.toString());

	}

	/**
	 * The SetFile instead of append(key, value) as just the key field
	 * append(key) and the value is always the NullWritable instance.
	 * 
	 * @throws IOException
	 */
	private void runSetFileExample() throws IOException {
		String dirName = "/tmp/hadoop/ch2-set-file-example/";
		FileSystem fs = FileSystemUtil.getFileSystemObject();
		Configuration conf = FileSystemUtil.getConfigurationObject();

		// Write a setfile
		Text txtKey = new Text();
		txtKey.set("JAMES!!!!!");
		Text txtKey2 = new Text();
		txtKey2.set("ANOTHER KEY!!!!");
		// Create an instance of MapFile.Writer and call append(), to add
		// key-values, in order.
		SetFile.Writer writer = new SetFile.Writer(conf, fs, dirName, txtKey.getClass(), CompressionType.NONE);
		writer.setIndexInterval(2);
		writer.append(txtKey2);
		writer.append(txtKey);
		writer.close();

		// Read a setfile
		SetFile.Reader reader = new SetFile.Reader(fs, dirName, conf);
		System.out.println(reader.getKeyClass().getName());
		reader.close();

	}

	/**
	 * The ArrayFile as just the value field append(value) and the key is a
	 * LongWritable that contains the record number, count + 1.
	 * 
	 * @throws IOException
	 */
	private void runArrayFileExample() throws IOException {
		String dirName = "/tmp/hadoop/ch2-ArrayFile-example/";
		FileSystem fs = FileSystemUtil.getFileSystemObject();
		Configuration conf = FileSystemUtil.getConfigurationObject();

		// Write a arrayfile
		Text txtValue = new Text();
		txtValue.set("JAMES!!!!!");
		Text txtValue2 = new Text();
		txtValue2.set("ANOTHER Value!!!!");
		// Create an instance of MapFile.Writer and call append(), to add
		// key-values, in order.
		ArrayFile.Writer writer = new ArrayFile.Writer(conf, fs, dirName, txtValue.getClass());
		writer.setIndexInterval(2);
		writer.append(txtValue2);
		writer.append(txtValue);
		writer.close();

		// Read a arrayfile
		ArrayFile.Reader reader = new ArrayFile.Reader(fs, dirName, conf);
		System.out.println(reader.getKeyClass().getName());
		// Create a Text object to store the result of the reads
		Text txtRead = new Text();
		System.out.println(reader.next(txtRead).toString());
		System.out.println(reader.next(txtRead).toString());
		reader.close();
	}

	private void runSequenceFileExample() throws IOException {

	}

	private void writeLanguageFileToHdfs() throws IOException {

		FileSystem fs = FileSystemUtil.getFileSystemObject();
		Path filePath = new Path("/tmp/hadoop/language.txt");

		if (fs.exists(filePath)) {
			fs.delete(filePath, false);
			System.out.println("Deleting file, it existed.");
		}

		System.out.println("Creating file:" + (fs.createNewFile(filePath) ? " passed" : " failed"));

		FSDataOutputStream out = fs.create(filePath);
		InputStream in = new BufferedInputStream(new FileInputStream(
				"C:\\dev-workspace\\hadoop-practice\\src\\main\\java\\hadoop\\practice\\book\\chapter2\\Languages.txt"));

		// Copy the bytes and close the in and output streams
		IOUtils.copyBytes(in, out, fs.getConf());
	}

	private void runTextFileExample() throws IOException {
		// ApplicationContext context = new
		// ClassPathXmlApplicationContext("spring/hadoop.xml");
		FileSystem fs = FileSystemUtil.getFileSystemObject();
		Path filePath = new Path("/tmp/hadoop/test.txt");

		// Delete the file if it exists
		if (fs.exists(filePath)) {
			fs.delete(filePath, false);
			System.out.println("Deleting file, it existed.");
		}

		// Create a new file
		Boolean result1 = fs.createNewFile(filePath);
		System.out.println("Creating file:" + (result1 ? " passed" : " failed"));

		if (fs.exists(filePath)) {
			if (fs.isFile(filePath)) {
				System.out.println("file path exists and is file");
			} else {
				System.out.println("file path exists but is not a file");
			}
		} else {
			System.out.println("file does not exist!");
		}

		// Print some details
		System.out.println("Some details: " + FileSystem.getAllStatistics());

		// Write a string to a file
		String content = "This is just a test.";
		System.out.println("Attempting to write contents to file: " + content);
		FSDataOutputStream out = fs.create(filePath);
		out.write(content.getBytes());
		out.close();
		System.out.println("Finished writing data to file output stream");

		// Get the contents of the file
		FSDataInputStream in = fs.open(filePath);
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		byte[] b = new byte[1];
		while (in.read(b) != -1) {
			bo.write(b);
		}
		in.close();
		String contentsRead = new String(bo.toByteArray());
		System.out.println("File contents from path: " + contentsRead);

	}
}
