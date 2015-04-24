package edu.buffalo.cse.ambience.database;

import java.util.ArrayList;

import orderly.Order;
import edu.buffalo.cse.ambience.dataStructures.Columns;
import edu.buffalo.cse.ambience.dataStructures.Rows;
import edu.buffalo.cse.ambience.dataStructures.gyan;

public class LibCassandra implements DBOps 
{

	@Override
	public boolean createTable(String tableName, String[] colfams) {
		// TODO Auto-generated method stub
		return false;
	}

	
	public boolean readTable(String tableName, String colfam) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean loadData(String tableName, Columns c, Rows r, String[] colFam) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean add() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean delete() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modify() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean scan() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ArrayList<gyan> topT(int T, int korder) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public ArrayList<gyan> topT(int T, int korder, Order sortOrder) {
		// TODO Auto-generated method stub
		return null;
	}

	

}
