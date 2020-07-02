
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;


	public class PROJECT1 {

	  //Map Phase
	    public static class FMapper extends Mapper<LongWritable, Text, Text, Text> {

	        private Text w = new Text();

	        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	            //Split data line by line
	        	//The first part as user name, second part is friend of user.
	        	String[] ip = value.toString().split("-"); 
	            if (ip.length == 2) {
	            	//Get the user
	                String friend1 = ip[0]; 
	                //Split user friend based on ','
	                List<String> values = Arrays.asList(ip[1].split(",")); 
	                //Group friends of each user
	                for (String friend2 : values)
	                {
	                   if (Integer.parseInt(friend1) < Integer.parseInt(friend2))
	                        w.set(friend1 + "," + friend2); 
	                    else
	                        w.set(friend2 + "," + friend1);
	                   //mapping users with common friend 
	                    context.write(w, new Text(ip[1]));
	                }
	            }
	        }

	    }
	    public static class FReducer extends Reducer<Text, Text, Text, Text>
	    {

	        private Text res = new Text();

	        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	        {
	        	// Create a hash set for each key-value
	            HashMap<String, Integer> FMap = new HashMap<String, Integer>();
	            // Create a StringBuilder for common friend
	            StringBuilder sb = new StringBuilder();
	            
	            //Reducing and Regrouping all key-value pairs
	            for (Text friends : values)
	            {
	                List<String> temporary = Arrays.asList(friends.toString().split(","));
	                for (String mutual : temporary)
	                {
	                    if (FMap.containsKey(mutual))
	                    	//Add friend to commom list
	                        sb.append(mutual + ',');
	                    else
	                        FMap.put(mutual, 1);

	                }
	            }
	            
	            //Output
	            res.set(new Text(sb.toString()));
	            context.write(key, res); 
	        }
	    }


	    //This is a driver Class
	    public static void main(String[] args) throws Exception
	    {
	        if (args.length != 2)
	        {
	        	//Only allow at least two user
	            System.err.println("ERROR!!! NOT ENOUGH INFOMATION TO EXECUTE"); 
	            System.exit(2);
	        }


	    }


	}
	
	

