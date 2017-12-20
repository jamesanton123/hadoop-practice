package hadoop.practice.book.chapter3;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
This needs to be refactered as per michaels suggestions
*/
public class ImageProcessor {	
	
	public static class ImageRedPixelMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
		IntWritable intW = new IntWritable();
		int redValue = 0;
		byte[] bytes;
		BufferedImage image;
		ByteArrayInputStream bais;
		int rTotal;
		int numPixels;
		int width;
		int height;
		int color;
		/**
		 * 
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			System.out.println("Starting a mapper");
			rTotal = 0;
			// Calculate the red average, and map it to the key			
			bais = new ByteArrayInputStream(value.copyBytes());			
			try {
		        image = ImageIO.read(bais);
		        System.out.println("Got image");
		    } catch (IOException e) {
		        e.printStackTrace();
		    }		
			System.out.println("Getting dimensions of image");
			if(null == image){
				System.out.println("Image was null");
			}
			width = image.getWidth();
			height = image.getHeight();
			System.out.println("Dimensions of image: " + width + ", " + height);
			numPixels = width * height;
			System.out.println("Beginning pixel loop");
			for(int x = 0; x < width; x++){
				for(int y = 0; y < height; y++){
					color = image.getRGB(x, y);					
					rTotal += (color & 0xff0000) >> 16;				
				}
			}
			redValue = rTotal / numPixels;
			intW.set(redValue);
			context.write(key, intW);
		}
		
	}

	public static class MaxRedReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		int curVal = 0;
		int maxRed = 0;
		Text key;
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {		
			while(values.iterator().hasNext()){				
				curVal = values.iterator().next().get();
				if(curVal > maxRed){
					this.key = key;
					this.maxRed = curVal;
				}
			}
			context.write(key, new IntWritable(){{set(maxRed);}});
		}
//		IntWritable val;
//		public void reduce(Text key, Iterable<IntWritable> values, Context context)	throws IOException, InterruptedException {
//			val = values.iterator().next();
//			context.write(key, val);
//		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "image processor");
		
		job.setJarByClass(ImageProcessor.class);
		job.setMapperClass(ImageRedPixelMapper.class);
		job.setReducerClass(MaxRedReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/tmp/hadoop/randomImages"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/hadoop/randImageOut"));		
		System.out.println("Job is about to run");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
