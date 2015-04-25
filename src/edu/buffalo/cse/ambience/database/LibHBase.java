package edu.buffalo.cse.ambience.database;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;

import orderly.DoubleWritableRowKey;
import orderly.Order;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.dataStructures.Columns;
import edu.buffalo.cse.ambience.dataStructures.Rows;
import edu.buffalo.cse.ambience.dataStructures.Table;
import edu.buffalo.cse.ambience.dataStructures.gyan;

public class LibHBase implements DBOps
{
	
	private Configuration conf = null;
	private Configuration hdfsConf = null;
	private static LibHBase instance = null;
	private static final int DEFAULT_SCANNER_CACHING=500;
	private boolean idem=false;
	private static final String[] alphabets = {"A","B","C","D","E","F","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"};
	private static final int alphaLen = alphabets.length;
	
	// configuration properties
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
	 * 
	 * @return
	 */
	public static LibHBase getInstance(Configuration hdfsConf) 
	{
		if(instance == null)
		{
			instance = new LibHBase(hdfsConf);
		}
		return instance;
	}
	
	
	/**
	 * 
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
		{
			return (int)Math.ceil((double)(i/alphaLen))+alphabets[(alphaLen-1)-i%(alphaLen)];
		}
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
			table = new HTable(conf, tableName);
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
   	 * @param colfam
   	 * @param T
   	 * @return
   	 */
   	public ResultScanner read(String tableName, String colfam)
	{
		try
		{
			HTable table = new HTable(conf, tableName);
			Scan scan = new Scan();
			scan.setCaching(100);
	
			scan.addFamily(Bytes.toBytes(colfam));
			ResultScanner scanner = table.getScanner(scan);
			return scanner;
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("Cannot read data from :"+tableName);
		}
		return null;
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
				//colDesc.setCompressionType(Compression.Algorithm.SNAPPY);
				tableDescriptor.addFamily(colDesc);
			}
			admin.createTable(tableDescriptor);
			admin.close();
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
	 * @param tableName
	 * @param colfams
	 * @param regions
	 * @param maxid
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
				//colDesc.setCompressionType(Compression.Algorithm.GZ);
				tableDescriptor.addFamily(colDesc);
			}
			admin.createTable(tableDescriptor,splits);
			admin.close();
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
	public HTable getTableHandler(String tableName) throws IOException
	{
		HTable tableH = null;
		tableH = new HTable(conf, tableName);
		return tableH;	
	}
	
	enum table_ops
	{
		add,
		print;
	}
	
	public boolean printJobStats(String jobID)
	{
		String tablename= AMBIENCE_tables.jobStats.getName()+jobID;
		String[] cf=AMBIENCE_tables.jobStats.getColFams();
		
		HTable table = null;
		
		try
		{
			table = new HTable(conf, tablename);
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
			table = new HTable(conf, tableName);
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
	 * Load data into HBase table as specified in the arguments
	 */
	public boolean loadData(String tbl,ArrayList<String> colnames,
			ArrayList<ArrayList<String>> rows,String colFams[],boolean loadInvalid) 
	{
		try
		{
			NavigableMap<HRegionInfo,ServerName> regions=getRegions(tbl);
			int splitCnt=regions.size();
			
			if(splitCnt > 1)
			{
				Iterator<String> keysIterator = getRegStartKeys(getRegions(tbl)).iterator();
				if(!keysIterator.hasNext()) return false;
				if(!loadPolyLith(tbl,colnames,rows,colFams,splitCnt,keysIterator,loadInvalid)) return false;
			}
			else
			{
				if(!loadMonoLith(tbl,colnames,rows,colFams,loadInvalid)) return false;
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
	
	private boolean loadMonoLith(String tbl,ArrayList<String> colnames,ArrayList<ArrayList<String>> rows,
			String[] colFams,boolean loadInvalid) throws IOException
	{
		HBaseAdmin hbAdmin = new HBaseAdmin(conf);
		HTable table=getTableHandler(tbl);
		int colSize = colnames.size()-1;
		byte[] indVars = Bytes.toBytes(colFams[0]);
		byte[] targetVar=Bytes.toBytes(colFams[1]);
		for(int i=0;i<rows.size();i++)
		{
			ArrayList<String> currentRow =  rows.get(i);
			Put objput = new Put(Bytes.toBytes("0"+i)); // to have all rows in order -- so that when we compare split vs !splits we have some notion
			for(int j=0;j<colSize;j++)
			{
				if(loadInvalid)
					objput.add(indVars, Bytes.toBytes(colnames.get(j)), Bytes.toBytes(currentRow.get(j)));
				else
				{
					if(!currentRow.get(j).equals("-99")&loadInvalid)
						objput.add(indVars, Bytes.toBytes(colnames.get(j)), Bytes.toBytes(currentRow.get(j)));
				}
				
			}
			objput.add(targetVar, Bytes.toBytes(colnames.get(colSize)), Bytes.toBytes(currentRow.get(colSize)));
			table.put(objput);
		}
		hbAdmin.close();
		table.close();
		return true;
	}
	
	private boolean loadPolyLith(String tbl,ArrayList<String> colnames,ArrayList<ArrayList<String>> rows,String colFams[],
			int splitCnt,Iterator<String> keysIt,boolean loadInvalid) throws IOException
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
				if(loadInvalid)
					objput.add(indVars, Bytes.toBytes(j), Bytes.toBytes(currentRow.get(j)));
				else
				{
					if(!currentRow.get(j).equals("-99"))
						objput.add(indVars, Bytes.toBytes(j), Bytes.toBytes(currentRow.get(j)));
				}
					
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
			HTable table = new HTable(conf, tableName);
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

	@Override
	public ArrayList<gyan> topT(String tblname,int T,int korder) 
	{
		int LIMIT=0;
		ArrayList<gyan> top=new ArrayList<gyan>(T);
	    ImmutableBytesWritable buffer = new ImmutableBytesWritable();
	    DoubleWritableRowKey d = new DoubleWritableRowKey();
	    HTable table=null;
	    try
		{
	    	table =getTableHandler(tblname);
			Scan scan = new Scan();
			scan.setCaching(DEFAULT_SCANNER_CACHING); 
			Filter korderFilter = new SingleColumnValueFilter(Bytes.toBytes("inforMet"),Bytes.toBytes("k"),CompareOp.EQUAL,Bytes.toBytes(Integer.toString(korder)));
			scan.setFilter(korderFilter);
			scan.addFamily(Bytes.toBytes("infoMet"));
			ResultScanner scanner = table.getScanner(scan);
			byte[] key;double value;
			NavigableMap<byte[], byte[]> rowKV;
			Get get;Result row;
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
			{
				LIMIT++;
				get = new Get(result.getRow());
				get.setMaxVersions(Integer.MAX_VALUE);
			    row = table.get(get);
			    key=result.getRow();
			    buffer.set(key, 0, key.length);
			    value =((DoubleWritable)d.deserialize(buffer)).get();
			    rowKV = row.getFamilyMap(Bytes.toBytes("infoMet"));
			    Set<byte[]> entry =  rowKV.keySet();
			    for(byte[] colKey : entry)
			    	top.add(new gyan(Bytes.toString(rowKV.get(colKey)),value));
			    if(LIMIT==T)break;
			}
			table.close();
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("Cannot read data from :"+tblname);
			try
			{
				if(table!=null) table.close();
			}
			catch(IOException ex)
			{
				System.out.println("some error in closing table");
			}
			return top;
		}
		
		return top;
	}
}