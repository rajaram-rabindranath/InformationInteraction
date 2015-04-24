package edu.buffalo.cse.ambience.HBase.MR.Mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import edu.buffalo.cse.ambience.dataStructures.Constants;

public class M_higherOder_simple extends TableMapper<Text,Text>
{
	static long numRecords=0;
	private String INVALID=null;
	private int k=0,n=0; // yes ..you guessed it right -- n choose k
	private String TARGET=null;
	private String src_cf[]=null;
	private int mapperID=0;
	private String mapLogK;
	private String mapLogV;
	private ArrayList<HashMap<Integer,String>> rows = new ArrayList<HashMap<Integer,String>>();
	private ArrayList<String> targets = new ArrayList<String>();
	private ArrayList<ComboObj> combList=new ArrayList<ComboObj>();
	
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
			n=Integer.valueOf(conf.get(MRParams.COMB_SETSIZE.toString()));
			String[] topComb=conf.get(MRParams.COMB_LIST.toString()).split(","); // FIXME -- DELIMITER
			for(int i=0;i<topComb.length;i++) // read in all the combinations as an array
				combList.add(new ComboObj(topComb[i]));
		}
		catch(NumberFormatException nex)
		{
			System.out.println("The combinations given are wrng!!!!"); // FIXME log4j
			nex.printStackTrace();
			throw nex;
		}		
	}
	
	public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException
    {
		
		HashMap<Integer,String> rowMap = new HashMap<Integer,String>();
		String targetValue =new String(values.getFamilyMap(Bytes.toBytes(src_cf[1])).get(Bytes.toBytes(TARGET)));
		NavigableMap<byte[],byte[]> origMap=values.getFamilyMap(Bytes.toBytes(src_cf[0]));
		int[] colMap=new int[origMap.keySet().size()];
		
    	System.out.println("Processing row "+Bytes.toString(row.get()));
    	boolean[] invalid=new boolean[combList.size()];
    	ArrayList<ComboObj> operList=chkValidity(origMap);
    	if(operList.isEmpty()) return;
    	buildVals(origMap);
    	/**
    	 * no point in doing this if we are not creating combinations
    	 */
    	int position=0;
		for(byte[] key:origMap.keySet()) // transform KV pairs 
		{
			rowMap.put(position,new String(origMap.get(key))); // mapping colID to index[0...n]
			colMap[position]=Bytes.toInt(key); // mapping index[0...n] to colID 
			position++;
		}
		
		StringBuilder strKey=new StringBuilder();
		StringBuilder strValue=new StringBuilder();
		boolean leftAppend=false;
		for(int i : rowMap.keySet()) // for all the keys in the row
		{
			int pos=colMap[i]; /// FIXME -- not for combinations
			System.out.println("combi ele "+i);
			for(ComboObj c : operList) // iterating thru each combination shared
			{
				strKey.setLength(0);
				strValue.setLength(0);
				System.out.print("processing "+c.strC);
				if(pos>c.max || (leftAppend=pos<c.min)) 
				{
					if(leftAppend)
					{
						strValue.append(rowMap.get(i));strValue.append(Constants.VAL_SEP);
						strValue.append(c.strV);
						strKey.append(pos);strKey.append(Constants.COMB_SEP);
						strKey.append(c.strC);
					}
					else // right append
					{
						strKey.append(c.strC);strKey.append(Constants.COMB_SEP);
						strKey.append(pos);
						strValue.append(c.strV);
						strValue.append(Constants.VAL_SEP);strValue.append(rowMap.get(i));
					}
					strValue.append(Constants.VAL_SEP);strValue.append(targetValue);
				}
				else // intersection or union
				{
					// create K,V pair
					int[] comb=c.intCArr;
					if(pos==c.max || pos==c.min)continue;
					//!<&!= 1st element
					int index=1;
					strKey.append(c.strCArr[0]);strKey.append(Constants.COMB_SEP);
					strValue.append(c.strVArr[0]);strValue.append(Constants.VAL_SEP);
					boolean bad=false;
					while(index<c.size)
					{
						if(comb[index]<pos)
						{
							strKey.append(c.strCArr[index]);strKey.append(Constants.COMB_SEP);
							strValue.append(c.strVArr[index]);strValue.append(Constants.VAL_SEP);
						}
						else if(comb[index]>pos)
							break;
						else 
						{
							bad=true;break;
						}
						index++;
					}// end while
					if(bad)continue;
					strKey.append(pos);strKey.append(Constants.COMB_SEP);
					strValue.append(rowMap.get(i));strValue.append(Constants.VAL_SEP);
					for(int l=index;l<c.size-1;l++)
					{
						strKey.append(c.strCArr[index]);strKey.append(Constants.COMB_SEP);
						strValue.append(c.strVArr[index]);strValue.append(Constants.VAL_SEP);
					}
					strKey.append(c.strCArr[c.size-1]);strValue.append(c.strVArr[c.size-1]);
					strValue.append(Constants.VAL_SEP);strValue.append(targetValue);
				}//end if else
				context.write(new Text(strKey.toString()),new Text(strValue.toString()+",1"));
			}// end for each CombList
		}// end for each row element
		numRecords++;
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
    {
		/********************** FOMULA FOR THE PAI*****************************
    	 *  calculating PAI(X1,X2,X3,P) = H(X1,X2,X3) + H(P) - H(X1,X2,X2,X3,P)
    	 **********************************************************************/
		System.out.println("Total number of records processed"+numRecords);
		/*mapLogK="map";
		mapLogV=mapperID+","+numRecords+","+iter;
		context.write(new Text(mapLogK),new Text(mapLogV));*/
	}
	
	
	/**
	 * 
	 * @param origMap
	 */
	private ArrayList<ComboObj> chkValidity(NavigableMap<byte[], byte[]> origMap)
	{
		ArrayList<ComboObj> n = new ArrayList<ComboObj>();
		Set<byte[]> keys=origMap.keySet();
		for(ComboObj c : combList)
		{
			if(!(origMap.size() <= c.size) &&!(Sets.intersection(c.byteArrSet,keys).size() < c.size))
				n.add(c);
		}
		return n;
	}
	
	private void buildVals(NavigableMap<byte[],byte[]> origMap)
	{
		StringBuilder val = new StringBuilder();
		for(ComboObj c : combList)
		{
			val.setLength(0);
			for(int i=0;i<c.size-1;i++)
			{
				c.strVArr[i]=Bytes.toString(origMap.get(c.byteCArr[i]));
				val.append(c.strVArr[i]);
				val.append(Constants.VAL_SEP);
			}
			c.strVArr[c.size-1]=Bytes.toString(origMap.get(c.byteCArr[c.size-1]));
			val.append(c.strVArr[c.size-1]);
			c.strV=val.toString();
		}
	}
	
	public void intersection()
	{
		
	}
	public void union()
	{
		
	}
}