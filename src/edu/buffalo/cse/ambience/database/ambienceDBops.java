package edu.buffalo.cse.ambience.database;

import java.util.ArrayList;

import orderly.Order;
import edu.buffalo.cse.ambience.dataStructures.ContingencyT;
import edu.buffalo.cse.ambience.dataStructures.gyan;

public interface ambienceDBops 
{
	public ArrayList<gyan> topT(int T,Order order);
	public double computeKWII(ContingencyT cTbl);
	public double computePAI(ContingencyT cTble);
	public ContingencyT getCTable(gyan g);
	public ArrayList<Double> computeKWII(ArrayList<gyan> listg);
	public double computeEntropy(gyan g);
}
