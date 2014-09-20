import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class PMICalculate {
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	        List<String> stopwords = readFromJARFile("/stopwords");
	        String ngram = value.toString();
	        String[] fields  = ngram.split("\t", -1);  
	        int year = Integer.parseInt(fields[1]);
	        	
	        if (year >= 1900)
	        {
	        	String[] parts = fields[0].split(" ");
	        	
	    		switch(Integer.valueOf(parts.length)) 
	    		{
			    	case 1: one_gram(map, parts);
			    	break;
			    	case 2: two_gram(map, parts);
			    	break;
			    	case 3: three_gram(map, parts);
			    	break;
			    	case 4: four_gram(map, parts);
			    	break;
			    	case 5: five_gram(map, parts);
			    	break;
			    	default:
			    	break;
	    		}
	        }
	      
	    }
	    
	    public List<String> readFromJARFile(String filename) throws IOException
		{
			  InputStream is = getClass().getResourceAsStream(filename);
			  InputStreamReader isr = new InputStreamReader(is);
			  BufferedReader br = new BufferedReader(isr);
			  List<String> stopwords = new Vector<String>();
			  String line;
			  while ((line = br.readLine()) != null) 
			  {
				  stopwords.add(line);
			  }
			  br.close();
			  isr.close();
			  is.close();	  
			  return stopwords;
		}
	  }	
	
	
	
	

	 public static void main(String[] args)
	 {
		Map<String, String> map = new HashMap<String, String>();
		String[] stopWords ={"after","the"};
		String word = "home";	
		
		
		List<String> keysList = new ArrayList<String>();
		

		}
				
		for (String string : stopWords) {
			if (map.containsKey(string))
			{
				map.remove(string);
			}			
		}
		
		for (Map.Entry<String, String> entry : map.entrySet())
		{
			if (!(entry.getKey().matches("[A-Za-z0-9]+")) || !(entry.getValue().matches("[A-Za-z0-9]+")))
			{
				keysList.add(entry.getKey());
			}
		}
		
		for (String string : keysList) 
		{
			map.remove(string);
		}
		
		System.out.println("Finished");
		
		for (Map.Entry<String, String> entry : map.entrySet())
		{
			System.out.println("Key:" + entry.getKey() +", Value:" + entry.getValue());
		}
		
				
	 }
	 
	 public static void one_gram(Map<String, String> map, String[] parts)
	 {
		 map.put("Ignore", "this");
	 }
	 
	 public static void two_gram(Map<String, String> map, String[] parts)
	 {
		 map.put(parts[0], parts[1]);
	 }
	 
	 public static void three_gram(Map<String, String> map, String[] parts)
	 {
		 map.put(parts[0], parts[1]);
		 map.put(parts[2], parts[1]);
	 }
	 
	 public static void four_gram(Map<String, String> map, String[] parts)
	 {
		 map.put(parts[0], parts[1]);
		 map.put(parts[2], parts[1]);
		 map.put(parts[3], parts[1]);
	 }
	 
	 public static void five_gram(Map<String, String> map, String[] parts)
	 {
		 map.put(parts[0], parts[2]);
		 map.put(parts[1], parts[2]);
		 map.put(parts[3], parts[2]);
		 map.put(parts[4], parts[2]);
	 }
	 
}
