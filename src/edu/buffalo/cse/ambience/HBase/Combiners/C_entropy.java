package edu.buffalo.cse.ambience.HBase.Combiners;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class C_entropy extends Reducer<Text, Text, Text, Text>
{
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		//Configuration conf = context.getConfiguration();
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		HashMap<String,Integer> valAggregator=new HashMap<String,Integer>();
	
		for(Text val : values) 
        {   
        	String a = val.toString();
        	// with mini combiner --
        	String[] splits = a.split(",");
        	int cnt = Integer.valueOf(splits[1]);
        	if(valAggregator.containsKey(splits[0]))
        		valAggregator.put(splits[0], valAggregator.get(splits[0])+cnt);
        	else
        		valAggregator.put(splits[0],cnt);
        }
        Set<String> vals = valAggregator.keySet();
        try
        {
        	for(String v : vals)
				context.write(key,new Text(v+","+valAggregator.get(v)));
		}
        catch(InterruptedException iex)
        {
        	iex.printStackTrace();
        }
    }
}
