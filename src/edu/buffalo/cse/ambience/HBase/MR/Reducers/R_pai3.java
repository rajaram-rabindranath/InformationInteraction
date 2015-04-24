package edu.buffalo.cse.ambience.HBase.MR.Reducers;

import java.io.IOException;

import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.math.Information;

public class R_pai3 extends AbstractReducer 
{
	/**
	 * 
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		/********************** FOMULA FOR THE PAI*************************************
    	 *  calculating PAI(X1,X2,X3,P) = H(X1,X2,X3) + H(P) - H(X1,X2,X2,X3,P)
    	 *  			PAI(KEY)		= H(combo)	  + H(target) - H(combo_n_target)		
    	 ******************************************************************************/
		HashBag combo =new HashBag();
		HashBag combo_n_target =new HashBag();
		HashBag target =new HashBag();
		StringBuilder comboVal=new StringBuilder();
		StringBuilder targetVal=new StringBuilder();
		int count=0;
		
		
		// debug code to check
		if(key.toString().equals("map"))
		{
			Put put;
			byte[] colfam_=Bytes.toBytes(AMBIENCE_tables.jobStats.getColFams()[0]);
			/*mapLogV=mapperID+","+numRecords+","+iter;*/
			for(Text val:values)
			{
				String strVal=val.toString();
				String[] splits=strVal.split(",");
				put=new Put(Bytes.toBytes(splits[0]));// key
				put.add(colfam_,Bytes.toBytes("#recs"),Bytes.toBytes(splits[1]));
				put.add(colfam_,Bytes.toBytes("#iter"),Bytes.toBytes(splits[2]));
				context.write(jobStatsT, put);
			}
			context.progress();
			return;
		}
		
		/************************************
		 * val eg. -->A|B|C|trait 1_2_1_0,1
		 ************************************/
		for(Text val : values) 
        {   
        	comboVal.setLength(0);
        	targetVal.setLength(0);
        	String[] split = val.toString().split(",");
        	int cnt = Integer.valueOf(split[1]);
        	String[] c =split[0].split(Constants.VAL_SPLIT);
        	targetVal.append(c[c.length-1]);
        	for(int i =0;i<c.length-1;i++)
        	{
        		comboVal.append(c[i]);
        		comboVal.append(Constants.VAL_SEP);
        	}
        	/** combo and target **/
        	combo_n_target.add(split[0],cnt);
        	combo.add(comboVal.toString(),cnt);
        	target.add(targetVal.toString(),cnt);
        	count+=cnt;
        }
		/*System.out.println("=============================="+key+"============================");
	    debug(combo_n_target);
	    debug(target);
	    debug(combo);*/
    	double PAI = Information.PAI(combo, target, combo_n_target, count);
    	
    	/**/
    	findT.add(key.toString(),PAI);
    	Put put = new Put(Bytes.toBytes(key.toString()));
    	put.add(colfam,qual,Bytes.toBytes(Double.toString(PAI)));
    	numkeys++;
    	context.write(sinkT, put);
    	context.progress();
    }
}
