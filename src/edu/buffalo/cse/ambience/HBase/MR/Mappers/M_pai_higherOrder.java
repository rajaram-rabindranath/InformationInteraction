package edu.buffalo.cse.ambience.HBase.MR.Mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.common.collect.Sets;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.Combination;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.dataStructures.comboStats;

public class M_pai_higherOrder extends TableMapper<Text,Text>
{
	static long numRecords=0;
	private String INVALID=null;
	private int k=0,n=0; // yes ..you guessed it right -- n choose k
	private String TARGET=null;
	private String src_cf[]=null;
	private int mapperID=0;
	private ArrayList<Combination> TopCombList=new ArrayList<Combination>();
	private Text txtKey=new Text();
	private Text txtVal=new Text(); 
	StringBuilder strKey=new StringBuilder();
	StringBuilder strValue=new StringBuilder();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException, NumberFormatException 
	{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		// just for checking debug
		String factor =conf.get("mapreduce.task.io.sort.factor");
		System.out.println("The map io sort factor "+factor);
		
		mapperID= context.getTaskAttemptID().getTaskID().getId();
		TARGET=conf.get(MRParams.TARGET_VAR.toString());
		INVALID=conf.get(MRParams.INVALID_VALUE.toString());
		src_cf=AMBIENCE_tables.source.getColFams();
		try
		{
			k=Integer.valueOf(conf.get(MRParams.K_WAY.toString()));
			n=Integer.valueOf(conf.get(MRParams.SET_SIZE.toString()));
			String[] topComb=conf.get(MRParams.TOP_COMBINATIONS.toString()).split(","); // FIXME -- DELIMITER
			for(int i=0;i<topComb.length;i++) // read in all the combinations as an array
				TopCombList.add(new Combination(topComb[i]));
		}
		catch(NumberFormatException nex)
		{
			System.out.println("The combinations given are wrng!!!!"); // FIXME log4j
			nex.printStackTrace();
			throw nex;
		}		
	}
	
	/**
	 * collect rows here to process them later in the cleanup method
	 */
	public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException
    {
		String targetValue =new String(values.getFamilyMap(Bytes.toBytes(src_cf[1])).get(Bytes.toBytes(TARGET)));
		NavigableMap<byte[],byte[]> origMap=values.getFamilyMap(Bytes.toBytes(src_cf[0]));
		
		/** 
		 * if we were to use filtering out of invalid values this
		 * would make sense -- not anymore
		 */
		ArrayList<Combination> operList=buildVals(origMap,TopCombList);
		if(operList.isEmpty()) return; // do not process this row -- if top combinations
    	int colID=0;
    	String val;
		for(byte[] key:origMap.keySet()) 
    	{
			val=Bytes.toString(origMap.get(key));
			if(val.equals(INVALID)) continue;
			colID=Bytes.toInt(key);
    		for(Combination comb: operList)
    		{
    			if(!comb.contains(colID))// not redundant
    			{ 
    				comb.genKV(colID, val, strKey, strValue);
    				strValue.append(targetValue);
    				txtVal.set(strValue.toString()+",1");
    				context.write(txtKey,txtVal);
    			}	
    		}
    	}
		numRecords++;
    }
	
	/**
	 * Do not process a combination for this row if: FIXME -- maybe we don't need this
	 * 1: # of cols in row is < the # of elements top combo
	 * 2: # intersecting cols is <  # of elements in combo
	 * @param origMap
	 */
	private ArrayList<Combination> chkValidity(NavigableMap<byte[], byte[]> origMap)
	{
		ArrayList<Combination> n = new ArrayList<Combination>();
		Set<byte[]> keys=origMap.keySet();
		for(Combination c : TopCombList)
		{
			if(!(origMap.size() <= c.size) &&!(Sets.intersection(c.byteArrSet,keys).size() < c.size))
				n.add(c);
		}
		return n;
	}
	
	/**
	 * 
	 * @param origMap
	 * @param list
	 * @return
	 */
	private ArrayList<Combination> buildVals(NavigableMap<byte[],byte[]> origMap,ArrayList<Combination> list)
	{
		StringBuilder val = new StringBuilder(); 
		ArrayList<Combination> qualified=new ArrayList<Combination>();
		boolean bad=false;
		for(Combination c : list)
		{
			val.setLength(0);
			for(int i=0;i<c.size-1;i++)
			{
				c.strVArr[i]=Bytes.toString(origMap.get(c.byteCArr[i]));
				if(c.strVArr[i].equals(INVALID))
				{
					bad=true;break;
				}
				
				val.append(c.strVArr[i]);
				val.append(Constants.VAL_SEP);
			}
			if(bad) // FIXME -- when we don't filter -99
			{
				bad=false;continue;
			}	
			c.strVArr[c.size-1]=Bytes.toString(origMap.get(c.byteCArr[c.size-1]));
			if(c.strVArr[c.size-1].equals(INVALID)) continue;
			val.append(c.strVArr[c.size-1]);
			c.strV=val.toString();
			qualified.add(c);
		}
		return qualified;
	}
}