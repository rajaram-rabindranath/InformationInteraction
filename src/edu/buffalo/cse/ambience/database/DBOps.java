package edu.buffalo.cse.ambience.database;

import java.util.ArrayList;

import orderly.Order;
import edu.buffalo.cse.ambience.dataStructures.Columns;
import edu.buffalo.cse.ambience.dataStructures.Rows;
import edu.buffalo.cse.ambience.dataStructures.gyan;

public interface DBOps 
{
	public boolean createTable(String tableName,String[] colfams);
	public boolean readTable(String tableName,String colfam);
	public boolean loadData(String tableName,Columns c,Rows r, String[] colFam);
	public boolean add();
	public boolean delete();
	public boolean modify();
	public boolean scan();
	public ArrayList<gyan> topT(int T,int korder);
	public ArrayList<gyan> topT(int T,int korder,Order sortOrder); 
}
