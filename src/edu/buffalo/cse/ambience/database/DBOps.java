package edu.buffalo.cse.ambience.database;

import java.util.ArrayList;

import orderly.Order;
import edu.buffalo.cse.ambience.dataStructures.Columns;
import edu.buffalo.cse.ambience.dataStructures.Rows;
import edu.buffalo.cse.ambience.dataStructures.gyan;

public interface DBOps 
{
	public boolean add();
	public boolean delete();
	public boolean modify();
	public boolean scan();
	public ArrayList<gyan> topT(String tblname,int T,int korder);
}
