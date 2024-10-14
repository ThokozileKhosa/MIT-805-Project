import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import javax.imageio.ImageIO;
import java.awt.Color;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ImageMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Map<String, Integer> colorCounts = new HashMap<>();

    // Define rounding factor for colour precision
    private final static int COLOUR_PRECISION = 50;
    
    // Define sampling rate
    private final static int PIXEL_SAMPLING_RATE = 20;

    // Maximum number of dominant colours to output
    private final static int MAX_DOMINANT_COLOURS = 3;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            // Decode the Base64 string to binary data
            byte[] imageData = Base64.getDecoder().decode(value.toString());

            // Convert byte array to BufferedImage
            ByteArrayInputStream bais = new ByteArrayInputStream(imageData);
            BufferedImage image = ImageIO.read(bais);

            if (image != null) {
                int width = image.getWidth();
                int height = image.getHeight();

                // Emit image dimensions
                context.write(new Text("Width: " + width), one);
                context.write(new Text("Height: " + height), one);

                // Extract and count colours with precision and sampling
                extractColourData(image);

                // Emit only dominant colours
                emitDominantColours(context);

                // Clear the map for the next image
                colorCounts.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void extractColourData(BufferedImage image) {
        int width = image.getWidth();
        int height = image.getHeight();

        // Loop over pixels with sampling and colours precision
        for (int y = 0; y < height; y += PIXEL_SAMPLING_RATE) {
            for (int x = 0; x < width; x += PIXEL_SAMPLING_RATE) {
                int rgb = image.getRGB(x, y);
                Color color = new Color(rgb);

                // Group similar colours by rounding RGB values (based on precision)
                int red = roundColourValue(color.getRed(), COLOUR_PRECISION);
                int green = roundColourValue(color.getGreen(), COLOUR_PRECISION);
                int blue = roundColourValue(color.getBlue(), COLOUR_PRECISION);

                // Create a colours string in the format "R,G,B"
                String colorKey = red + "," + green + "," + blue;

                // Update the colours count
                colorCounts.put(colorKey, colorCounts.getOrDefault(colorKey, 0) + 1);
            }
        }
    }

    private int roundColourValue(int value, int precision) {
        return (value / precision) * precision;
    }

    private void emitDominantColours(Context context) throws IOException, InterruptedException {
        // Get the dominant colours by frequency
        Map<String, Integer> topColors = colorCounts.entrySet()
            .stream()
            .sorted((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue())) // Sort by frequency
            .limit(MAX_DOMINANT_COLOURS) 
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
            ));

        // Emit the dominant colours
        for (Map.Entry<String, Integer> entry : topColors.entrySet()) {
            context.write(new Text("Color: " + entry.getKey()), new IntWritable(entry.getValue()));
        }
    }
}
