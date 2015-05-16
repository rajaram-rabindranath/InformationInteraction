package edu.buffalo.cse.ambience.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import orderly.Order;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.dataStructures.ContingencyT;
import edu.buffalo.cse.ambience.dataStructures.gyan;
import edu.buffalo.cse.ambience.database.LibHBase;
import edu.buffalo.cse.ambience.math.Information;

public class AMBIENCE_metrics implements ambienceDBops
{

	private LibHBase HBase;
	
	public AMBIENCE_metrics(LibHBase HBase)
	{
		this.HBase=HBase;
	}
	
	/**
	 * 
	 */
   	@Override
   	public double getKWII(String vars,String delim) throws IOException
   	{
   		gyan g = new gyan(HBase.getID(vars, delim),vars);
   		return getKWII(g);
   	}
   	
   	public double getKWII(gyan g) throws IOException
	{
   		
   		ContingencyT ctbl=getCTable(g);
   		return Information.KWII(ctbl,g.korder);
	}

   	
   	/**
   	 * 
   	 */
   	@Override
   	public double getPAI(String vars,String delim) throws IOException 
   	{
		gyan g = new gyan(HBase.getID(vars, delim),vars);
		return getPAI(g);
   	}
   	public double getPAI(gyan g) throws IOException
	{
		ContingencyT ctbl=getCTable(g);
		return Information.PAI(ctbl);
	}

	/**
	 * 
	 */
	@Override
	public double getEntropy(String vars,String delim) throws IOException
	{
		gyan g = new gyan(HBase.getID(vars, delim),vars);
		return getEntropy(g);
   	}
	
	public double getEntropy(gyan g)
	{
		ContingencyT ctbl=getCTable(g);
		return  Information.Entropy(ctbl);
	}
	
	
	@Override
	public ArrayList<gyan> getKWII(ArrayList<String> list,String delim) throws IOException
	{
		ArrayList<gyan> listg=new ArrayList<gyan>();
		for(String s: list)
		{
			listg.add(new gyan(HBase.getID(s, delim),s));
		}
		return getKWII(listg);
	}
	
	private ArrayList<gyan> getKWII(ArrayList<gyan> listg) throws IOException
	{
		HashMap<gyan,ContingencyT> ctblMap=new HashMap<gyan,ContingencyT>();
		for(gyan g:listg)
			ctblMap.put(g,getCTable(g));
		for(gyan n : ctblMap.keySet())
			n.value=Information.KWII(ctblMap.get(n),n.korder);
		return null;
	}

	
	/**
	 * 
	 */
	@Override
	public ContingencyT getCTable(String vars,String delim) throws IOException
	{
		gyan g = new gyan(HBase.getID(vars, delim),vars);
		return getCTable(g);
	}
	
	private ContingencyT getCTable(gyan g)
	{
		ContingencyT ctbl=null;
		String contTbl=AMBIENCE_tables.contingency.getName();
		if(HBase.tableExists(contTbl)) return null;
		try
		{
			HTable ctblHandle=HBase.getTableHandler(contTbl);
			if(!g.isIDTranslated)
				g.combID=HBase.getID(g.comb,Constants.COMB_SPLIT);
			byte[] colfam=Bytes.toBytes(AMBIENCE_tables.contingency.getColFams()[0]);
			NavigableMap<byte[],byte[]> map=ctblHandle.get(new Get(Bytes.toBytes(g.combID))).getFamilyMap(colfam);
	   		ctbl=new ContingencyT(map);
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("SOME PROBLEMs GETTINg CONTINGENCY TBALE!!!");
			return ctbl;
		}
		return ctbl;
	}
	
	@Override
	public ArrayList<gyan> topT(int T,Order order)
	{
		ArrayList<gyan> top=new ArrayList<gyan>();
		try
		{
			HTable table = HBase.getTableHandler("top");
			Scan scan = new Scan();
			scan.setCaching(T); 
			scan.setMaxVersions(Integer.MAX_VALUE);
			ResultScanner scanner = table.getScanner(scan);
			int LIMIT=0;
			String key;
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
			{
				double metric=HBase.orderedRep(result.getRow(),Order.DESCENDING);
			    List<Cell> values =result.getColumnCells(Bytes.toBytes("DATA"),Bytes.toBytes("ID"));
			    for(Cell c: values)
			    {	
			    	key=Bytes.toString(CellUtil.cloneValue(c));
			    	top.add(new gyan(key,HBase.getVar(key,Constants.COMB_SPLIT),metric));
			    	if(++LIMIT==T)return top;
			    }
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("Problem accessing topT!!");
		}
		return top;
	}
}
