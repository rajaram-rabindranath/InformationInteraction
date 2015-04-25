package edu.buffalo.cse.ambience.HBase.MR.Mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import edu.buffalo.cse.ambience.core.AMBIENCE;
import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.Combination;
import edu.buffalo.cse.ambience.dataStructures.Constants;

public class M_kwiiList extends TableMapper<Text,Text>
{
	static int numRecords=0;
	private String INVALID=null;
	private int k=0,n=0; // yes ..you guessed it right -- n choose k
	private String src_cf[]=null;
	private ArrayList<HashMap<Integer,String>> rows = new ArrayList<HashMap<Integer,String>>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		INVALID=conf.get(MRParams.INVALID_VALUE.toString());
		src_cf=AMBIENCE_tables.source.getColFams();
		try
		{
			k=Integer.valueOf(conf.get(MRParams.K_WAY.toString()));
			n=Integer.valueOf(conf.get(MRParams.SET_SIZE.toString()));
		}
		catch(NumberFormatException nex)
		{
			nex.printStackTrace();
			System.out.println("either the value of N or K or both are bad!!");
			throw nex;
		}
	}
	
	/**
	 * Just a collector of records
	 */
	public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException 
    {
		/*if(k==0 || n ==0) --FIXME -- must test with bad values for K and N
		{
			System.out.println("ERROR ::");
			System.out.println("given "+n+" choose "+k);
			return;
		}*/
		/**
		 * keep pocketing all the rows into rowMap -- for a flourish at the end [clean-up]
		 * much like the last 10 overs of ONE-DAY Cricket - i tell you
		 */
		HashMap<Integer,String> rowMap= AMBIENCE.basicTransform(values.getFamilyMap(Bytes.toBytes(src_cf[0])));
    	rows.add(rowMap);
    	numRecords++;
    }
	
	@Override
	/**
	 * act as a in-mapper combiner -- lets see how this works
	 */
    protected void cleanup(Context context) throws IOException, InterruptedException 
    {
		/********************** FOMULA FOR THE KWII*****************************
    	 *  
    	 **********************************************************************/
		boolean esc=false;
    	int[] comb= new int[k];
    	for(int i=0;i<k;i++)
    		comb[i] = i;
    	StringBuilder strValue = new StringBuilder();
    	StringBuilder strKey = new StringBuilder();
    	
		HashMap<Integer, String> curRow=null;
		HashMap<String,Integer> miniCombiner=new HashMap<String,Integer>();
		Text key=null;
		
		if(numRecords != rows.size())
		{
			System.out.println("We have a fundamental problem");
			System.out.println("\n\n\n");
		}
		/**************************************************
		 * For each combination in *N choose K*
		 * A. Create key
		 * B. iterate thru each row -- and generate values
		 **************************************************/
		do
		{
			strKey.setLength(0);
			miniCombiner = new HashMap<String,Integer>(); // don't use mincombiner.clear()  
			for(int i=0;i<comb.length-1;i++)
			{
				strKey.append(comb[i]);
				strKey.append(Constants.COMB_SEP);
			}
			strKey.append(comb[comb.length-1]);
			key=new Text(strKey.toString()); 
			
			// iterate thru rows
			for(int rowIndex=0;rowIndex<rows.size();rowIndex++)
			{
				curRow = rows.get(rowIndex);
				strValue.setLength(0);
				for(int i=0;i<comb.length-1;i++)
	        	{
					if(curRow.get(comb[i]).equals(INVALID))
	        		{
	        			esc=true;
	        			break;
	        		}
	        		strValue.append(curRow.get(comb[i]));
					strValue.append(Constants.VAL_SEP);
	        	}
				if(!esc && !(curRow.get(comb[comb.length-1]).equals(INVALID)))
	    		{
					strValue.append(curRow.get(comb[comb.length-1]));
					String val = strValue.toString();
					if(miniCombiner.containsKey(val))
						miniCombiner.put(val,(miniCombiner.get(val))+1);
					else
						miniCombiner.put(val,1);
				}
				esc=false;
			}
			
			Set<String> values = miniCombiner.keySet();
			for(String val : values)
			{
				context.write(key,new Text(val+","+miniCombiner.get(val)));
			}
		}while(AMBIENCE.nextCombination(comb, n, k));
		System.out.println("the total number of records processed by this mapper ---"+ numRecords);// FIXME -- log4j
    }
}