package edu.buffalo.cse.ambience.HBase.MR.Mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import edu.buffalo.cse.ambience.core.AMBIENCE;
import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.Constants;

public class M_pai_cont extends TableMapper<Text,Text>
{
	static int numRecords=0;
	private String INVALID=null;
	private int k=0,n=0; // yes ..you guessed it right -- n choose k
	private String TARGET=null;
	private String src_cf[]=null;
	
	private ArrayList<HashMap<Integer,String>> rows = new ArrayList<HashMap<Integer,String>>();
	private ArrayList<String> targets = new ArrayList<String>();
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		TARGET=conf.get(MRParams.TARGET_VAR.toString());
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
		String targetValue =new String(values.getFamilyMap(Bytes.toBytes(src_cf[1])).get(Bytes.toBytes(TARGET)));
    	HashMap<Integer,String> rowMap=AMBIENCE.basicTransform(values.getFamilyMap(Bytes.toBytes(src_cf[0])));
    	rows.add(rowMap);
    	targets.add(targetValue);
    	numRecords++;
    }
	
	@Override
	/**
	 * act as a in-mapper combiner -- lets see how this works
	 */
    protected void cleanup(Context context) throws IOException, InterruptedException 
    {
		/********************** FOMULA FOR THE PAI*****************************
    	 *  calculating PAI(X1,X2,X3,P) = H(X1,X2,X3) + H(P) - H(X1,X2,X2,X3,P)
    	 **********************************************************************/
		boolean esc=false;
    	int[] comb= new int[k];
    	for(int i=0;i<k;i++)
    		comb[i] = i;
    	StringBuilder strValue = new StringBuilder();
    	StringBuilder strKey = new StringBuilder();
    	
		HashMap<Integer, String> curRow=null;
		HashBag miniCombiner=new HashBag();
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
			miniCombiner.clear();
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
				for(int i=0;i<comb.length;i++)
	        	{
					if(curRow.get(comb[i]).equals(INVALID))
	        		{
	        			esc=true;
	        			break;
	        		}
	        		strValue.append(curRow.get(comb[i]));
					strValue.append(Constants.VAL_SEP);
	        	}
				if(!esc)
	    		{
					strValue.append(targets.get(rowIndex));
					String val = strValue.toString();
					miniCombiner.add(val);
				}
				esc=false;
			}
			
			Set<String> values = miniCombiner.uniqueSet();
			for(String val : values)
			{
				context.write(key,new Text(val+","+miniCombiner.getCount(val)));
			}
		}while(AMBIENCE.nextCombination(comb, n, k));
		System.out.println("the total number of records processed by this mapper ---"+ numRecords);// FIXME -- log4j
    }
}