package edu.buffalo.cse.ambience.HBase.Combiners;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class C_pai extends Reducer<Text, Text, Text, Text>
{
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		//Configuration conf = context.getConfiguration();
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		//HashMap<String,Integer> valAggregator=new HashMap<String,Integer>();]
		if(key.toString().equals("map"))
		{
			for(Text val:values)
			{
				context.write(key,val);
			}
			return;
		}
		
		
		/*
		 * A|B|C -- Key
		 * 1_2_3_0,10 --> value
		 * splits[1] == 10
		 * splits[0] == 1_2_3_0
		 */
		HashBag aggregator =  new HashBag();
		for(Text val : values) 
        {   
        	String a = val.toString();
        	String[] splits = a.split(",");
        	int cnt = Integer.valueOf(splits[1]);
        	aggregator.add(splits[0],cnt);
        	
        	/*if(aggregator.containsKey(splits[0]))
        		valAggregator.put(splits[0], valAggregator.get(splits[0])+cnt);
        	else
        		valAggregator.put(splits[0],cnt);*/
        }
        //Set<String> vals = valAggregator.keySet();
        Set<String> vals = aggregator.uniqueSet();
		try
        {
        	for(String v : vals)
        	{
        		context.write(key,new Text(v+","+aggregator.getCount(v)));
        	}
				//context.write(key,new Text(v+","+valAggregator.get(v)));
		}
        catch(InterruptedException iex)
        {
        	iex.printStackTrace();
        }
    }
}
