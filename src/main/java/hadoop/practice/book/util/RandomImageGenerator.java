package hadoop.practice.book.util;

import java.io.File;
import java.io.IOException;
import java.awt.image.BufferedImage;
import javax.imageio.ImageIO;

public class RandomImageGenerator {

	
	public static void generateImage(String outputFilePath, int width, int height){
		BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
		File f = null;
		for (int y = 0; y < height; y++) {
			for (int x = 0; x < width; x++) {
				int a = (int) (Math.random() * 256); // alpha
				int r = (int) (Math.random() * 256); // red
				int g = (int) (Math.random() * 256); // green
				int b = (int) (Math.random() * 256); // blue
				int p = (a << 24) | (r << 16) | (g << 8) | b; // pixel
				img.setRGB(x, y, p);
			}
		}
		try {
			f = new File(outputFilePath);
			ImageIO.write(img, "png", f);
		} catch (IOException e) {
			System.out.println("Error: " + e);
		}
	}
}

