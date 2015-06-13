package edu.buffalo.cse.ambience.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import orderly.Order;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.dataStructures.ContingencyT;
import edu.buffalo.cse.ambience.dataStructures.gyan;
import edu.buffalo.cse.ambience.database.ElementNotFoundException;
import edu.buffalo.cse.ambience.database.LibHBase;
import edu.buffalo.cse.ambience.database.TableNotFoundException;
import edu.buffalo.cse.ambience.math.Information;

public class AMBIENCE_metrics implements ambienceDBops
{

	private LibHBase DBase; // FIXME must be an interface with all the common functions
	private static final byte[] kwiiFam=Bytes.toBytes(AMBIENCE_tables.mutualInfo.getColFams()[0]);
	private static final byte[] paiFam=Bytes.toBytes(AMBIENCE_tables.mutualInfo.getColFams()[0]);
	private static final byte[] kwiiQual=Bytes.toBytes(Constants.kwiiQual);
	private static final byte[] paiQual=Bytes.toBytes(Constants.paiQual);
	
	public AMBIENCE_metrics(LibHBase DBase)
	{
		this.DBase=DBase;
	}
	
	/**
	 * 
	 */
   	@Override
   	public double getKWII(String vars,String delim) throws IOException,NumberFormatException,ElementNotFoundException
   	{
   		String combID=DBase.getID(vars, delim);
   		String miTable=AMBIENCE_tables.mutualInfo.getName();
   		byte[] rslt;
   		try
   		{
   			if((rslt=DBase.getRecord(miTable,combID).getValue(kwiiFam,kwiiQual))==null)
   			{
   				gyan g = new gyan(combID);
   	   			return getKWII(g);
   			}	
   			return Double.valueOf(Bytes.toString(rslt));
   		}
   		catch(TableNotFoundException tnfex)
   		{
   			gyan g = new gyan(combID);
   			return getKWII(g);
   		}
   	}
   	
   	public double getKWII(gyan g) throws IOException,NumberFormatException,ElementNotFoundException
	{
   		ContingencyT ctbl=getCTable(g);
   		return Information.KWII(ctbl,g.getKorder());
   	}

   	
   	/**
   	 * 
   	 */
   	@Override
   	public double getPAI(String vars,String delim) throws IOException,NumberFormatException,ElementNotFoundException
   	{
   		String combID=DBase.getID(vars, delim);
   		String miTable=AMBIENCE_tables.mutualInfo.getName();
   		byte[] rslt;
   		try
   		{
   			if((rslt=DBase.getRecord(miTable,combID).getValue(paiFam,paiQual))==null)
   			{
   				gyan g = new gyan(combID);
   	   			return getKWII(g);
   			}	
   			return Double.valueOf(Bytes.toString(rslt));
   		}
   		catch(TableNotFoundException tnfex)
   		{
   			gyan g = new gyan(combID);
   			return getPAI(g);
   		}
   	}
   	public double getPAI(gyan g) throws IOException,NumberFormatException,ElementNotFoundException
	{
		ContingencyT ctbl=getCTable(g);
		return Information.PAI(ctbl);
   	}

	/**
	 * 
	 */
	@Override
	public double getEntropy(String vars,String delim) throws IOException,NumberFormatException,ElementNotFoundException
	{
		gyan g =new gyan(DBase.getID(vars, delim));
   		return getEntropy(g);
	}
	
	public double getEntropy(gyan g) throws IOException,ElementNotFoundException,NumberFormatException
	{
		ContingencyT ctbl=getCTable(g);
		return  Information.Entropy(ctbl);
	}
	
	
	@Override
	public ArrayList<gyan> getKWII(ArrayList<String> list,String delim) throws IOException,NumberFormatException
	{
		ArrayList<gyan> listg=new ArrayList<gyan>();
		for(String s: list)
		{
			try
			{
				listg.add(new gyan(DBase.getID(s, delim)));
			}
			catch(ElementNotFoundException enfex)
			{
				System.out.println(s+" not found");
			}
		}
		return getKWII(listg);
	}
	
	public ArrayList<gyan> getKWII(ArrayList<gyan> listg) throws IOException,NumberFormatException
	{
		HashMap<gyan,ContingencyT> ctblMap=new HashMap<gyan,ContingencyT>();
		for(gyan g:listg)
		{
			try
			{ctblMap.put(g,getCTable(g));}
			catch(ElementNotFoundException enfex)
			{
				System.out.println(g.getCombination()+" does not exist!");
			}
		}
		for(gyan g : ctblMap.keySet())
			g.value=Information.KWII(ctblMap.get(g),g.getKorder());
		return null;
	}

	
	/**
	 * 
	 */
	@Override
	public ContingencyT getCTable(String vars,String delim) throws IOException,ElementNotFoundException,NumberFormatException
	{
		gyan g = new gyan(DBase.getID(vars, delim));
		return getCTable(g);
	}
	
	/**
	 * 
	 * @param g
	 * @return
	 */
	private ContingencyT getCTable(gyan g) throws IOException,NumberFormatException,ElementNotFoundException
	{
		ContingencyT ctbl=null;
		String contTbl=AMBIENCE_tables.contingency.getName();
		byte[] colfam=Bytes.toBytes(AMBIENCE_tables.contingency.getColFams()[0]);
		try
		{
			ctbl=new ContingencyT(DBase.getRecord(contTbl,g.getCombination()).getFamilyMap(colfam));
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("SOME PROBLEMS GETTING CONTINGENCY TBALE!!!");
			return ctbl;
		}
		catch(TableNotFoundException tnfex)
		{
			System.out.println("Contingency Table does not exist!");
			tnfex.printStackTrace();
			throw new ElementNotFoundException();
		}
		return ctbl;
	}
	
	@Override
	public ArrayList<gyan> topT(int T,Order order)
	{
		ArrayList<gyan> top=new ArrayList<gyan>();
		AMBIENCE_tables topTbl = AMBIENCE_tables.top;  
		byte[] qual=Bytes.toBytes("ID"); // FIXME --- HARDCODED
		byte[] colfam=Bytes.toBytes(topTbl.getColFams()[0]);
		try
		{
			HTable table = DBase.getTableHandler(topTbl.getName()+"4"); // FIXME
			Scan scan = new Scan();
			scan.setCaching(T); 
			scan.setMaxVersions(Integer.MAX_VALUE);
			ResultScanner scanner = table.getScanner(scan);
			int LIMIT=0;
			String key;
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
			{
				double metric=DBase.orderedRep(result.getRow(),order);
			    List<Cell> values =result.getColumnCells(colfam,qual);
			    for(Cell c: values)
			    {	
			    	key=Bytes.toString(CellUtil.cloneValue(c));
			    	top.add(new gyan(key,metric));
			    	if(++LIMIT==T)return top;
			    }
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("Problem accessing `topT!!");
		}
		catch(TableNotFoundException tnfex)
		{
			System.out.println("Top table does not exist");
			tnfex.printStackTrace();
		}
		return top;
	}
}
