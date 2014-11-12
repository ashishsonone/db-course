/*
* To change this template, choose Tools | Templates
* and open the template in the editor.
*/


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


class TextArrayWritable extends ArrayWritable {
	public TextArrayWritable() { 
		super(Text.class);
	}
	
	@Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String[] words = super.toStrings(); 
        Arrays.sort(words);
        for (int i=0; i<words.length; i++)
        {
            sb.append(words[i]);
            if(i < words.length-1) sb.append(", ");
        }
        return sb.toString();
    }
}
/**
*
* @author sapna
*/
public class WordCount {

public static class Map extends Mapper<LongWritable, Text, Text, TextArrayWritable > {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString()," ,().?'`\"!@#$%^&*:;<>/"); // line to string token
		Text sorted_key = new Text(); // type of output key
		
		Text word = new Text();
		
		ArrayList<String> tokens = new ArrayList<String>();
		
		//store all lowercase words
		while (itr.hasMoreTokens()) {
			String str = itr.nextToken();
			str = str.toLowerCase();
			tokens.add(str);
		}
		
		//single words
		for(int i=0; i<tokens.size(); i++){
			String str = tokens.get(i);
			
			word.set(str);
			ArrayList<Text> wordlist = new ArrayList<Text>();
			wordlist.add(word); //unsorted word is the value
			
			TextArrayWritable wordlistwritable = new TextArrayWritable();
			wordlistwritable.set(wordlist.toArray(new Text[wordlist.size()]));
			
			char[] chars = str.toCharArray();
	        Arrays.sort(chars);
	        String sorted = new String(chars);
			sorted_key.set(sorted); // set word as each input keyword
//			System.out.println(sorted_key + " " + word);
			context.write(sorted_key, wordlistwritable); // create a pair <keyword, 1>
		}
		
		//double words
		for(int i=0; i<tokens.size()-1; i++){
			String w1 = tokens.get(i);
			String w2 = tokens.get(i+1);
			
			String str_space = w1 + " " + w2;
			String str = w1 + w2;
			
			word.set(str_space);
			ArrayList<Text> wordlist = new ArrayList<Text>();
			wordlist.add(word); //unsorted spaced words is the value
			
			TextArrayWritable wordlistwritable = new TextArrayWritable();
			wordlistwritable.set(wordlist.toArray(new Text[wordlist.size()]));
			
			char[] chars = str.toCharArray(); //key
	        Arrays.sort(chars);
	        String sorted = new String(chars);
			sorted_key.set(sorted); // set word as each input keyword
//			System.out.println(sorted_key + " : " + word);
			context.write(sorted_key, wordlistwritable); // create a pair <keyword, 1>
		}
	}
}

public static class Reduce extends Reducer<Text, TextArrayWritable, NullWritable, TextArrayWritable> {

	public void reduce(Text key, Iterable<TextArrayWritable> values,Context context) throws IOException, InterruptedException {
		
		HashMap<Text, Integer> map= new HashMap<Text,Integer>();
		
//		System.out.println("key : " + key);
		for (TextArrayWritable list : values) {
			Text[] array = (Text[]) list.toArray();
			for(int i=0; i<array.length; i++){
				
				Text word = array[i];
//				System.out.println("    " + word);
				
				if(!map.containsKey(word)){
					map.put(word, 1);
				}
			}
		}
		//Now form a list with unique elements only
		ArrayList<Text> newlist = new ArrayList<Text>();
		
		Set<Text> distinct_words = map.keySet();
		if(distinct_words.size() <= 1) return;
	    
		// Display elements
	    for(Text w : distinct_words){
	    	newlist.add(w);
	    }
	    
	    TextArrayWritable wordlistwritable = new TextArrayWritable();
		wordlistwritable.set(newlist.toArray(new Text[newlist.size()]));
	    
	    context.write(NullWritable.get(), wordlistwritable);
	}
}

// Driver program
public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
	if (otherArgs.length != 2) {
		System.err.println("Usage: WordCount <in> <out>");
		System.exit(2);
	}

	// 	create a job with name "wordcount"
	Job job = new Job(conf, "wordcount");
	job.setJarByClass(WordCount.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);

	// 	uncomment the following line to add the Combiner
	//	job.setCombinerClass(Reduce.class);


	// set output key type
	job.setOutputKeyClass(Text.class);
	// set output value type
	job.setOutputValueClass(TextArrayWritable.class);
	
	//set the HDFS path of the input data
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	// 	set the HDFS path for the output
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	//Wait till job completion
	System.exit(job.waitForCompletion(true) ? 0 : 1);
 }//end of main method


}//end of class word count 