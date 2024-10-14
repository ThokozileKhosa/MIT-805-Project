import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProductReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Counter for items
        int count = 0; 
        double totalPrice = 0.0;
        Map<String, Integer> colourCountMap = new HashMap<>();
        Map<String, Integer> seasonCountMap = new HashMap<>();
        Map<String, Integer> genderCountMap = new HashMap<>();

        for (Text val : values) {
            String[] fields = val.toString().split(",");
            try {
                double price = Double.parseDouble(fields[0]);
                String colour = fields[1];
                String season = fields[2];
                String gender = fields[3];

                // Sum prices and count items
                totalPrice += price;
                count++;

                // Count colours
                colourCountMap.put(colour, colourCountMap.getOrDefault(colour, 0) + 1);

                // Count seasons
                seasonCountMap.put(season, seasonCountMap.getOrDefault(season, 0) + 1);
                
                // Count dominant genders
                genderCountMap.put(gender, genderCountMap.getOrDefault(gender, 0) + 1);

            } catch (ArrayIndexOutOfBoundsException e) {
                System.err.println("Array index out of bounds for value: " + val);
            } catch (NumberFormatException e) {
                System.err.println("Failed to parse price: " + fields[0]);
            }
        }

        // Added check
        if (count > 0) {
            // Calculate average price
            double avgPrice = totalPrice / count;

            // Find dominant colours
            String topColour1 = colourCountMap.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(1)
                    .map(Map.Entry::getKey)
                    .findFirst().orElse("None");

            String topColour2 = colourCountMap.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(topColour1))
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(1)
                    .map(Map.Entry::getKey)
                    .findFirst().orElse("None");

            // Find most frequent season
            String dominantSeason = seasonCountMap.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .get().getKey();
            
            // Find most frequent gender
            String dominantGender = genderCountMap.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .get().getKey();

            // Output format: key, Price, topColour1, topColour2, dominantSeason, dominantGender, totalCount
            context.write(key, new Text(String.format("%.2f,%s,%s,%s,%s,%d", avgPrice, topColour1, topColour2, dominantSeason, dominantGender, count)));
        
        } else {
            System.out.println("No valid records for key: " + key);
        }
    }
}
