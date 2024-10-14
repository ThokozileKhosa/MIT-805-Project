import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split(",");

	    if (fields.length >= 12) { // Check for at least 12 fields
	        String articleType = fields[9].trim();  // articleType field
	        String price = fields[3].trim();        // price field
	        String colour = fields[11].trim();       // colour field
	        String season = fields[13].trim();      // season field
            String gender = fields[15].trim();       // gender field

	        // Key will be articleType, Value will be a concatenation of price, colour, and season
	        if (!articleType.isEmpty() && !price.isEmpty()) {
	            context.write(new Text(articleType), new Text(price + "," + colour + "," + season + "," + gender));
	        }
	    }
	}
  }
