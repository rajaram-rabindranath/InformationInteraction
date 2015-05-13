package edu.buffalo.cse.ambience.database;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import orderly.DoubleWritableRowKey;
import orderly.Order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;

import com.sun.corba.se.pept.transport.InboundConnectionCache;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.dataStructures.Columns;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.dataStructures.ContingencyT;
import edu.buffalo.cse.ambience.dataStructures.Rows;
import edu.buffalo.cse.ambience.dataStructures.Table;
import edu.buffalo.cse.ambience.dataStructures.gyan;

public class LibHBase implements DBOps, ambienceDBops
{
	private Configuration conf=null;
	private Configuration hdfsConf=null;
	private static LibHBase instance=null;
	private static final int DEFAULT_SCANNER_CACHING=500;
	private boolean idem=false;
	private static final String[] alphabets={"A","B","C","D","E","F","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"};
	private static final int alphaLen=alphabets.length;
	private static String tblSuffix="";
	private HashMap<String,HTable> tables=new HashMap<String,HTable>(); // HTable creation is costly 
	private static final long DEFAULT_TIMEOUT=1800000*2; 
	
	/**
	 * From experience -- FIXME
	 * @author dev
	 */
	private enum MRplatformParams
	{
		
	}
	
	private LibHBase(Configuration hdfsConf) 
	{
		this.hdfsConf=hdfsConf;
		conf = HBaseConfiguration.create(hdfsConf);//HBaseConfiguration.create(); -- FIXME -- lib jars
		// set all job properties
		conf.setLong("mapreduce.task.timeout", DEFAULT_TIMEOUT);
		conf.setBoolean("mapreduce.map.speculative",false); 
		//conf.set("mapreduce.task.io.sort.factor",)
	}
	
	/**
	 * @return
	 */
	public static LibHBase getInstance(Configuration hdfsConf) 
	{
		if(instance == null)
			instance = new LibHBase(hdfsConf);
		return instance;
	}
	
	
	/**
	 * @param milliseconds
	 */
	public void setTimeout(long milliseconds)
	{
		conf.setLong("mapreduce.task.timeout", milliseconds);
	}
	
	/**
	 * 
	 * @return
	 */
	public Configuration getConf()
	{
		return conf;
	}

	public void setTblSuffix(String suffix)
	{
		tblSuffix=suffix;
	}
	/**
	 * 
	 * @param serverCnt
	 * @return
	 */
	private byte[][] genSplits(int splitCnt)
	{
		byte[][] regionSplits=new byte[splitCnt-1][];
		for(int i=0;i<splitCnt-1;i++)
		{
			regionSplits[i] = Bytes.toBytes(getRegionBoundary(i+1));
		}
		return regionSplits;
	}
	
	/**
	 * 
	 * @param i
	 * @return
	 */
	private String getRegionBoundary(int i)
	{
		if(i > alphaLen-1)
			return (int)Math.ceil((double)(i/alphaLen))+alphabets[(alphaLen-1)-i%(alphaLen)];
		return alphabets[i];
	}
	
	/**
   	 * 
   	 * @param tableName
   	 * @return
   	 * @throws IOException
   	 */
   	public NavigableMap<HRegionInfo,ServerName> getRegions(String tableName) throws IOException
   	{
   		HTable handle = getTableHandler(tableName);
   		return handle.getRegionLocations();
   	}
   	
   	/**
   	 * Given a set of variable names (combination) return their ids [in sorted order]
   	 * @param var
   	 * @return
   	 * @throws IOException
   	 */
	public ArrayList<String> getID(ArrayList<String> many,String delim) throws IOException,NumberFormatException
   	{
   		ArrayList<String> trans=new ArrayList<String>();
   		String rslt;
   		for(String t : many)
   		{
   			if((rslt=getID(t,delim))!=null)
   				trans.add(rslt);
   		}
   		return trans;
   	}
	
	/**
	 * 
	 * @param combination
	 * @param delim
	 * @return
	 * @throws IOException
	 * @throws NumberFormatException
	 */
   	public String getID(String combination,String delim) throws IOException,NumberFormatException
   	{
   		if(combination==null) return null;
   		String[] splits= combination.split(delim);
   		if(splits.length==1){System.out.println("INVALID DELIMITER FOR "+combination);}
   		String[] rslt=getID(splits);
   		StringBuilder b =new StringBuilder();
   		b.setLength(0);
		for(int i=0;i<rslt.length-1;i++)
		{
			b.append(rslt[i]);b.append(Constants.COMB_SEP);
		}
		b.append(rslt[rslt.length-1]);
   		return b.toString();
   	}
   	
   	/**
   	 * 
   	 * @param var
   	 * @return
   	 * @throws IOException
   	 */
   	private String[] getID(String... var) throws IOException
	{
   		String fwdMap=AMBIENCE_tables.fwdMap.getName()+tblSuffix;
   		String[] fwdMapCF=AMBIENCE_tables.fwdMap.getColFams();
   		byte[] colfam=Bytes.toBytes(fwdMapCF[0]);
   		byte[] qual=Bytes.toBytes(Constants.mapTblQual);
   		HTable tbl = getTableHandler(fwdMap) ;
		String[] mapped=new String[var.length];
		Get g;
		for(int i=0;i<var.length;i++)
		{
			g = new Get(Bytes.toBytes(var[i]));
			mapped[i]=Bytes.toString(tbl.get(g).getValue(colfam,qual));
			if(mapped[i]==null) return null; // if entity not present
		}
		sort(mapped);
		return mapped;
	}
   	
   	
	/**
   	 * Given a set of ids returns variable names 
   	 * @param ids
   	 * @return
   	 * @throws IOException
   	 */
   	public ArrayList<String> getVar(ArrayList<String> many,String delim) throws IOException,NumberFormatException
   	{
   		ArrayList<String> trans=new ArrayList<String>();
   		String rslt;
   		for(String t : many)
   		{
   			if((rslt=getVar(t,delim))!=null)
   				trans.add(rslt);
   		}
   		return trans;
   	}
   	
   	/**
   	 * 
   	 * @param combination
   	 * @param delim
   	 * @return
   	 * @throws IOException
   	 * @throws NumberFormatException
   	 */
   	private String getVar(String combination,String delim) throws IOException,NumberFormatException
   	{
   		if(combination==null) return null;
   		String[] splits= combination.split(delim);
   		if(splits.length==1){System.out.println("INVALID DELIMITER FOR "+combination);}
   		String[] rslt=getVar(splits);
   		StringBuilder b =new StringBuilder();
   		b.setLength(0);
		for(int i=0;i<rslt.length-1;i++)
		{
			b.append(rslt[i]);b.append(Constants.COMB_SEP);
		}
		b.append(rslt[rslt.length-1]);
   		return b.toString(); 
   	}
   	
   	/**
   	 * 
   	 * @param ids
   	 * @return
   	 * @throws IOException
   	 */
   	public String[] getVar(String... ids) throws IOException
	{
   		String revMap=AMBIENCE_tables.revMap.getName()+tblSuffix;
   		String[] revMapCF=AMBIENCE_tables.revMap.getColFams();
   		byte[] colfam=Bytes.toBytes(revMapCF[0]);
   		byte[] qual=Bytes.toBytes(Constants.mapTblQual);
   		HTable tbl= getTableHandler(revMap) ;
   		sort(ids);
		String[] mapped=new String[ids.length];
		Get g;
		for(int i=0;i<mapped.length;i++)
		{
			g = new Get(Bytes.toBytes(ids[i]));
			mapped[i]=Bytes.toString(tbl.get(g).getValue(colfam,qual));
			if(mapped[i]==null) return null; // if entity not present
		}
		return mapped;
	}
   	
   	
   	/**
   	 * 
   	 */
   	private void findCommon(int[][] candidates)
   	{
   		
   	}
   	
   
   	/**
   	 * Non MR way of counting table row count
   	 * @param tableName
   	 */
   	public long getRowCnt(String tableName,String colfam)
   	{
   		HTable table = null;
		long rowCnt=0;
		System.out.println("-------------- Table Row cnt ---------------");
		try
		{
			table = getTableHandler(tableName);
			Scan scan = new Scan();
			scan.setCaching(DEFAULT_SCANNER_CACHING); 
	
			scan.addFamily(Bytes.toBytes(colfam));
			ResultScanner scanner = table.getScanner(scan);
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
				rowCnt++;
			table.close();
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("Cannot read data from :"+tableName);
			try
			{
				if(table!=null) table.close();
			}
			catch(IOException ex)
			{
				System.out.println("some error in closing");
			}
		}
		return rowCnt;
	}
   	
   	/**
   	 * 
   	 * @return
   	 */
   	public ArrayList<String> getRegStartKeys(NavigableMap<HRegionInfo,ServerName> regions)
   	{
   		ArrayList<String> regionKeys=new ArrayList<String>();
   		Set<HRegionInfo> regSet = regions.keySet();
		for(HRegionInfo hr : regSet)
    	{
    		if(Bytes.toStringBinary(hr.getStartKey()).isEmpty()) // hack
    			regionKeys.add("0");
    		else
    			regionKeys.add(Bytes.toStringBinary(hr.getStartKey()));
    	}
		return regionKeys;
	}
   	
	/**
	 * 
	 * @param tableFilter
	 * @return
	 */
	public Scan getScanner(ArrayList<String> tableFilter)
	{
		Scan s = new Scan();
		String src_cf[] = AMBIENCE_tables.source.getColFams();
		s.addFamily(Bytes.toBytes(src_cf[0]));
		s.setCaching(DEFAULT_SCANNER_CACHING); 
		s.setCacheBlocks(false); // always set false for MR jobs
		if(tableFilter!=null)
		{
			byte[] fam= Bytes.toBytes(src_cf[0]);
			for(String col : tableFilter)
			{
				s.addColumn(fam, Bytes.toBytes(col));
		    }
		}
		return s;
	}
	
	public Scan getScanner(int cacheSize,Filter filter) // FIXME
	{
		return null;
	}
	
	
	public Scan getScanner(ArrayList<String> tableFilter, int cacheCnt)
	{
		Scan s=getScanner(tableFilter);
		s.setCaching(cacheCnt);
		return s;
	}
	
	public void setRejectVal(Scan s,String value)
	{
		Filter negFilter=new ValueFilter(CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(value)));
		s.setFilter(negFilter);
	}
	
	public void setAcceptVal(Scan s,String value)
	{
		Filter negFilter=new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(value)));
		s.setFilter(negFilter);
	}
	
	
	public void setColLimit(Scan s, int colLimt)
	{
		Filter filter = new ColumnCountGetFilter(colLimt);
		s.setFilter(filter);
	}
	
	/**
	 * 
	 * @return
	 */
	public Scan getScanner()
	{
		Scan s = new Scan();
		String src_cf[] = AMBIENCE_tables.source.getColFams();
		s.addFamily(Bytes.toBytes(src_cf[0]));
		s.setCaching(DEFAULT_SCANNER_CACHING); 
		return s;
	}
	
   	/**
   	 * 
   	 * @param tableName
   	 * @return
   	 */
   	public boolean tableExists(String tableName)
   	{
   		HBaseAdmin admin=null;;
   		try
   		{
   			admin = new HBaseAdmin(conf);
   			//admin.getConnection().
   			boolean rslt = admin.tableExists(tableName);
   			admin.close();
   			return rslt;
   		}
   		catch(MasterNotRunningException mex)
   		{
   			System.out.println("Master not running exception!!");
   			mex.printStackTrace();
   		}
   		catch(ZooKeeperConnectionException zex)
   		{
   			System.out.println("Zookeeper connection exception !!");
   			zex.printStackTrace();
   		}
   		catch (IOException e)
   		{
   			System.out.println("IO exception !!");
   			e.printStackTrace();
   		}
   		finally
   		{
   			try
   			{
	   			if(admin!=null)
	   				admin.close();
   			}
   			catch(IOException iex)
   			{
   				System.out.println("HAdmin is null!!!");
   			}
   		}
   		return false;
   	}
   	
   	
   	
   /**
	 * Create table in Hbase for storing data
	 */
	public boolean createTable(String tableName, String[] colfams) 
	{
		Boolean retVal= true;
		HColumnDescriptor colDesc;
		HBaseAdmin admin=null;
		try
		{
			admin = new HBaseAdmin(conf);
			// delete table if it already exists
			if(admin.tableExists(tableName) == true)
			{
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			for(int i =0;i<colfams.length;i++)
			{
				colDesc=new HColumnDescriptor(colfams[i]);
				tableDescriptor.addFamily(colDesc);
			}
			admin.createTable(tableDescriptor);
			admin.close();
			tables.put(tableName,getTableHandler(tableName)); // avoid creating many table handler
		}
		catch(MasterNotRunningException mex)
   		{
   			System.out.println("Master not running exception!!");
   			mex.printStackTrace();
   		}
   		catch(ZooKeeperConnectionException zex)
   		{
   			System.out.println("Zookeeper connection exception !!");
   			zex.printStackTrace();
   		}
   		catch (IOException e)
   		{
   			System.out.println("IO exception !!");
   			e.printStackTrace();
   		}
   		finally
   		{
   			try
   			{
	   			if(admin!=null)
	   				admin.close();
   			}
   			catch(IOException iex)
   			{
   				System.out.println("HAdmin is null!!!");
   			}
   		}
		
		return retVal;
	}

	/**
	 * OverLoaded method
	 * 
	 * @return
	 */
	public boolean createTable(String tableName, String[] colfams,int splitCnt) 
	{
		Boolean retVal= true;
		HColumnDescriptor colDesc;
		HBaseAdmin admin=null;
		try
		{
			byte[][] splits=genSplits(splitCnt);
			admin = new HBaseAdmin(conf);
			if(admin.tableExists(tableName) == true)
			{
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			
			for(int i =0;i<colfams.length;i++)
			{
				colDesc=new HColumnDescriptor(colfams[i]);
				//System.out.println("Just trying out compression");
				//colDesc.setCompressionType(Compression.Algorithm.LZO);
				tableDescriptor.addFamily(colDesc);
			}
			admin.createTable(tableDescriptor,splits);
			admin.close();
			tables.put(tableName,getTableHandler(tableName));
		}
		catch(MasterNotRunningException mex)
   		{
   			System.out.println("Master not running exception!!");
   			mex.printStackTrace();
   		}
   		catch(ZooKeeperConnectionException zex)
   		{
   			System.out.println("Zookeeper connection exception !!");
   			zex.printStackTrace();
   		}
   		catch (IOException e)
   		{
   			System.out.println("IO exception !!");
   			e.printStackTrace();
   		}
   		finally
   		{
   			try
   			{
	   			if(admin!=null)
	   				admin.close();
   			}
   			catch(IOException iex)
   			{
   				System.out.println("HAdmin is null!!!");
   			}
   		}
		return retVal;
	}

	/**
	 * 
	 * @param tableName
	 * @return
	 */
	public HTable getTableHandler(String tblname) throws IOException
	{
		//HTable table = tables.get(tblname);
		//if(table==null)
		return new HTable(conf,tblname);
		//return table;	
	}
	
	public boolean printJobStats(String jobID)
	{
		String tablename= AMBIENCE_tables.jobStats.getName()+jobID;
		String[] cf=AMBIENCE_tables.jobStats.getColFams();
		HTable table = null;
		try
		{
			table = getTableHandler(tablename);
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.addFamily(Bytes.toBytes(cf[0]));
			System.out.println("------------- Mapper Stats ------------");
			ResultScanner scanner = table.getScanner(scan);
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
			{
				// handling mutual info table
				Get get = new Get(result.getRow());
			    Result entireRow = table.get(get);
			    String key = Bytes.toString(result.getRow());
			    NavigableMap<byte[], byte[]> list = entireRow.getFamilyMap(Bytes.toBytes(cf[0]));
			    Set<byte[]> entry =  list.keySet();
			    System.out.print("MapperID="+key);
			    //#keys=4,#n=4,#rec=6
			    for(byte[] colKey : entry)
            	{
			    	System.out.print(Bytes.toString(colKey)+"="+Bytes.toString(list.get(colKey))+",");
			    }
			    System.out.println();
			}
			scan = new Scan();
			scan.setCaching(500);
			scan.addFamily(Bytes.toBytes(cf[0]));
			scan.addFamily(Bytes.toBytes(cf[1]));
			scanner = table.getScanner(scan);
			System.out.println("------------- Reducer Stats ------------");
			long acc=0;
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
			{
				// handling mutual info table
				Get get = new Get(result.getRow());
			    Result entireRow = table.get(get);
			    String key = Bytes.toString(result.getRow());
			    NavigableMap<byte[], byte[]> list = entireRow.getFamilyMap(Bytes.toBytes(cf[1]));
			    Set<byte[]> entry =  list.keySet();
			    System.out.print("ReducerID="+key);
			    for(byte[] colKey : entry)
            	{
			    	System.out.println(Bytes.toString(colKey)+"="+Bytes.toString(list.get(colKey))+",");
			    	acc+=Integer.valueOf(Bytes.toString(list.get(colKey)));
			    }
			}
			System.out.println("Total # of keys processed by reducer "+acc);
			table.close();
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("Cannot read data from :"+tablename);
			try
			{
				if(table!=null) table.close();
			}
			catch(IOException ex)
			{
				System.out.println("some error in closing");
			}
			return false;
		}
		return true;
	}
	
	public boolean readTable(String tableName, String colfam) 
	{
		System.out.println("========================"+tableName+"=======================");
		Table data = Table.getInstance();
		HTable table = null;
		try
		{
			table = getTableHandler(tableName);
			Scan scan = new Scan();
			scan.setCaching(DEFAULT_SCANNER_CACHING); 
			scan.addFamily(Bytes.toBytes(colfam));
			ResultScanner scanner = table.getScanner(scan);
			int counter=0;
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
			{
				if(counter==10)
				{
					counter++;
					break;
				}
			    Get get = new Get(result.getRow());
			    Result entireRow = table.get(get);
			    String key = Bytes.toString(result.getRow());
			    NavigableMap<byte[], byte[]> list = entireRow.getFamilyMap(Bytes.toBytes(colfam));
			    Set<byte[]> entry =  list.keySet();
			    
			    for(byte[] colKey : entry)
            	{
			    		System.out.println("Key="+key+"--"+"value="+Bytes.toString(list.get(colKey)));
			    }
			    counter++;
			}
			table.close();
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("Cannot read data from :"+tableName);
			try
			{
				if(table!=null) table.close();
			}
			catch(IOException ex)
			{
				System.out.println("some error in closing");
			}
			return false;
		}
		return true;
	}

	/**
	 * Creates read-only tables for forward and reverse mapping 
	 * 
	 * @param tblname
	 * @param colnames
	 * @param ColFams
	 * @return
	 */
	public boolean setupMapping(ArrayList<String> colnames,String tblSuffix) // FIXME -- in normal environs not required suffix
	{
		String tblFdwMap=AMBIENCE_tables.fwdMap.getName()+tblSuffix;
		String tblRevMap=AMBIENCE_tables.revMap.getName()+tblSuffix;
		byte[] fmapCf=Bytes.toBytes(AMBIENCE_tables.fwdMap.getColFams()[0]);
		byte[] rmapCf=Bytes.toBytes(AMBIENCE_tables.revMap.getColFams()[0]);
		byte[] qual=Bytes.toBytes(Constants.mapTblQual);
		if(!createTable(tblFdwMap,AMBIENCE_tables.fwdMap.getColFams())) return false;
		if(!createTable(tblRevMap,AMBIENCE_tables.revMap.getColFams())) return false;
		HTable fmap=null,rmap=null;
		try
		{
			fmap=getTableHandler(tblFdwMap);
			rmap=getTableHandler(tblRevMap);
			Put fput=null,rput=null;
			String id,col;
			for(int i=0;i<colnames.size()-1;i++) // must not add trait as well !!
			{
				id=Integer.toString(i);
				col=colnames.get(i);
				fput=new Put(Bytes.toBytes(col));
				fput.add(fmapCf,qual,Bytes.toBytes(id));
				rput=new Put(Bytes.toBytes(id));
				rput.add(rmapCf,qual,Bytes.toBytes(col));
				fmap.put(fput);rmap.put(rput);
			}
			tables.put(tblFdwMap,fmap);
			tables.put(tblRevMap,rmap);
			/*fmap.getTableDescriptor().setReadOnly(true);
			rmap.getTableDescriptor().setReadOnly(true);*/
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("MAPPING TABLES FAILURE!");
			return false;
		}
		finally
		{
			try
			{
				if(fmap!=null)fmap.close();
				if(rmap!=null)rmap.close();
			}
			catch(IOException ioex)
			{
				ioex.printStackTrace();
				System.out.println("PROBLEMS CLOSING MAPPING TABLES!");
			}
		}
		return true;
	}
	
	/**
	 * Load data into HBase table as specified in the arguments
	 */
	public boolean loadData(String tbl,ArrayList<String> colnames,
			ArrayList<ArrayList<String>> rows,String colFams[]) 
	{
		try
		{
			NavigableMap<HRegionInfo,ServerName> regions=getRegions(tbl);
			int splitCnt=regions.size();
			if(splitCnt > 1)
			{
				Iterator<String> keysIterator = getRegStartKeys(getRegions(tbl)).iterator();
				if(!keysIterator.hasNext()) return false;
				if(!loadPolyLith(tbl,colnames,rows,colFams,splitCnt,keysIterator)) return false;
			}
			else
			{
				if(!loadMonoLith(tbl,colnames,rows,colFams)) return false;
			}
		}
		catch(IOException iex)
		{
			System.out.println("Problems in loading data to "+tbl);
			iex.printStackTrace();
			return false;
		}
		return true;
		
	}
	
	private boolean loadMonoLith(String tbl,ArrayList<String> colnames,
			ArrayList<ArrayList<String>> rows,String[] colFams) throws IOException
	{
		HBaseAdmin hbAdmin = new HBaseAdmin(conf);
		HTable table=getTableHandler(tbl);
		int colSize = colnames.size()-1;
		byte[] indVars = Bytes.toBytes(colFams[0]);
		byte[] targetVar=Bytes.toBytes(colFams[1]);
		for(int i=0;i<rows.size();i++)
		{
			ArrayList<String> currentRow =  rows.get(i);
			Put objput = new Put(Bytes.toBytes(i));
			for(int j=0;j<colSize;j++)
			{
				objput.add(indVars, Bytes.toBytes(Integer.toString(j)), Bytes.toBytes(currentRow.get(j)));
			}
			objput.add(targetVar, Bytes.toBytes(colnames.get(colSize)), Bytes.toBytes(currentRow.get(colSize)));
			table.put(objput);
		}
		hbAdmin.close();
		table.close();
		return true;
	}
	
	private boolean loadPolyLith(String tbl,ArrayList<String> colnames,ArrayList<ArrayList<String>> rows,String colFams[],
			int splitCnt,Iterator<String> keysIt) throws IOException
	{
		HBaseAdmin hbAdmin = new HBaseAdmin(conf);
		HTable table = getTableHandler(tbl);
		int colSize = colnames.size()-1;
		int rowCnt=rows.size();
		int chunkSize=(int)Math.floor((double)rowCnt/splitCnt),chunkMod=rowCnt%splitCnt;
		boolean moduloPresent=true;
		if(chunkMod!=0) chunkSize++;
		else
			moduloPresent=false;
		byte[] indVars = Bytes.toBytes(colFams[0]);
		byte[] targetVar=Bytes.toBytes(colFams[1]);
		int suffix=1;
		String prefix=keysIt.next();
		String rowKey="";
		for(int i=0;i<rows.size();i++)
		{
			ArrayList<String> currentRow =  rows.get(i);
			rowKey=prefix+suffix;
			Put objput = new Put(Bytes.toBytes(rowKey));
			for(int j=0;j<colSize;j++)
			{
				objput.add(indVars, Bytes.toBytes(Integer.toString(j)), Bytes.toBytes(currentRow.get(j)));
			}
			objput.add(targetVar, Bytes.toBytes(colnames.get(colSize)), Bytes.toBytes(currentRow.get(colSize)));
			table.put(objput);
			suffix++;
			if(suffix > chunkSize && keysIt.hasNext() )
			{
				System.out.println("for prefix ="+prefix+" rec cnt="+(suffix-1));
				if(moduloPresent) // when #splits unevenly divides #row 
				{
					chunkMod--;
					if(chunkMod <= 0)
						chunkSize = idempotent(chunkSize);
				}
				prefix= keysIt.next();
				suffix=1;
			}
		}
		System.out.println("for prefix ="+prefix+" rec cnt="+(suffix-1));
		hbAdmin.close();
		table.close();
		return true;
	}
	
	
	
	/**
	 * 
	 * @param s
	 * @return
	 */
	private int idempotent(int s)
	{
		if(!idem)
		{
			idem=true;
			return (s-1);
		}
		return s;
	}
	
	/**
	 * 
	 * @param tableName
	 * @param RowKey
	 * @return
	 */
	public boolean getRecord(String tableName,String RowKey)
	{
		// check what form of encoding is done
		Result rs = null;
		try
		{
			HTable table = getTableHandler(tableName);
	        Get get = new Get(RowKey.getBytes());
	        rs = table.get(get);
	        if(rs == null || rs.isEmpty())
	        {
	        	return false;
	        }
	    }
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * 
	 * @param map
	 */
	public void displayRegionInfo(String tbl) throws IOException
	{
		NavigableMap<HRegionInfo,ServerName> regions=getRegions(tbl);
		Set<HRegionInfo> n = regions.keySet();
		System.out.println("------------------- Region Info ------------------");
		System.out.println("# of regions = "+n.size());
		System.out.println("REGION_ID\tSERVER");
		for(HRegionInfo hr : n)
    		System.out.println(hr.getRegionId()+"\t"+regions.get(hr).getServerName());
    }
	
	@Override
	public boolean add() 
	{
		return false;
	}

	@Override
	public boolean delete() 
	{
		return false;
	}

	@Override
	public boolean modify() 
	{
		return false;
	}

	@Override
	public boolean scan() 
	{
		return false;
	}

	

	private static double orderedRep(byte[] k,Order order) throws IOException
	{
		ImmutableBytesWritable buffer = new ImmutableBytesWritable();
	    DoubleWritableRowKey d = new DoubleWritableRowKey();
	    d.setOrder(order);
	    buffer.set(k, 0, k.length);
	    return ((DoubleWritable)d.deserialize(buffer)).get();
	}
	
	
	
	@Override
	public ArrayList<gyan> topT(int T,Order order)
	{
		ArrayList<gyan> top=new ArrayList<gyan>();
		try
		{
			HTable table = getTableHandler("top");
			Scan scan = new Scan();
			scan.setCaching(T); 
			scan.setMaxVersions(Integer.MAX_VALUE);
			ResultScanner scanner = table.getScanner(scan);
			int LIMIT=0;
			String key;
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
			{
				double metric=orderedRep(result.getRow(),Order.DESCENDING);
			    List<Cell> values =result.getColumnCells(Bytes.toBytes("DATA"),Bytes.toBytes("ID"));
			    for(Cell c: values)
			    {	
			    	key=Bytes.toString(CellUtil.cloneValue(c));
			    	top.add(new gyan(key,getVar(key,Constants.COMB_SPLIT),metric));
			    	if(++LIMIT==T)return top;
			    }
			}
			table.close();
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("Problem accessing topT!!");
		}
		return top;
	}
	
	@Override
	public double computeKWII(ContingencyT cTbl) 
	{
		//ascertain order of the combination
		return 0;
	}
	
	@Override
	public double computePAI(ContingencyT cTbl) 
	{
		//ascertain order of the combination
		return 0;
	}

	@Override
	public ContingencyT getCTable(gyan g) 
	{
		// ascertain order ---
		
		// -- connect to contingency table and get contingency
		
		// -- fetch
		
		return null;
	}

	@Override
	public ArrayList<Double> computeKWII(ArrayList<gyan> listg) 
	{
		//ascertain order of the combination
		
		return null;
	}

	@Override
	public double computeEntropy(gyan g) 
	{
		// TODO Auto-generated method stub
		return 0;
	}
	
	public boolean exit()
	{
		// close all table handler before leaving
		return true;
	}
	
	
	/**
   	 * 
   	 * @param mapped
   	 * @return
   	 */
   	private void sort(String[] ele)
   	{
   		int length=ele.length;
   		int[] sorted=new int[ele.length];
   		for(int i=0;i<length;i++)
   			sorted[i]=Integer.valueOf(ele[i]);
   		sort(sorted,0,length-1);
   		for(int i=0;i<ele.length;i++)
   				ele[i]=Integer.toString(sorted[i]);
   	}
   	
   	/**
   	 * 
   	 * @param vals
   	 * @return
   	 */
   	private void sort(int[] array,int lowerIndex, int higherIndex)
   	{
   		int i = lowerIndex;
        int j = higherIndex;
        int pivot = array[lowerIndex+(higherIndex-lowerIndex)/2];
        while (i <= j) 
        {
            while (array[i] < pivot) 
                i++;
            while (array[j] > pivot) 
                j--;
            if (i <= j)
            {
            	int temp = array[i];
                array[i] = array[j];
                array[j] = temp;
                i++;
                j--;
            }
        }
        if (lowerIndex < j)
            	sort(array,lowerIndex, j);
        if (i < higherIndex)
            	sort(array,i, higherIndex);
   	}
   	
   	
   	public void tstMapping() throws IOException, NumberFormatException
   	{
   		ArrayList<String> tst=new ArrayList<String>();
		tst.add("15|0|1");
		tst.add("15|1|0");
		tst.add("1|15|0");
		tst.add("1|0|15");
		tst.add("0|1|15");
		tst.add("0|15|1");
		ArrayList<String> str;
		str=getVar(tst, "\\|");
		for(String s: str)
		{
			System.out.println("we are getting "+s);
		}
		
		tst.clear();
		
		tst.add("044892|044356|044376");
		tst.add("044892|044376|044356");
		tst.add("044376|044892|044356");
		tst.add("044376|044356|044892");
		tst.add("044356|044376|044892");
		tst.add("044356|044892|044376");
		str = getID(tst,"\\|");
		for(String s: str)
		{
			System.out.println("we are getting "+s);
		}
   	}
}