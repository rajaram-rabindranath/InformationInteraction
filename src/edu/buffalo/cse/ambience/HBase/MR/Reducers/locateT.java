package edu.buffalo.cse.ambience.HBase.MR.Reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

import orderly.DoubleWritableRowKey;
import orderly.Order;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.DoubleWritable;

import edu.buffalo.cse.ambience.dataStructures.gyan;



/**
 * 
 * @author dev
 *
 */
enum order
{
	top,
	bottom;
	public static order getOrder(String str)
	{
		if(str==null) return top;
		try
		{
			return valueOf(str.toUpperCase());
		}
		catch(Exception ex)
		{
			System.out.println("DEFAULT ");
			return top;
		}
	}
}

public abstract class locateT 
{
	protected PriorityQueue<gyan> PQ;
	protected int T;
	abstract public void add(String combID,double value);
	abstract protected void setOrder();
	abstract public ArrayList<gyan> asList();
	
	private locateT(int T,Comparator<gyan> cmp)
	{
		this.T=T;
		PQ=new PriorityQueue<gyan>(T,cmp);
	}
	
	public static locateT getInstance(int size,order where)
	{
		locateT locT=null;
		switch(where)
		{
			case top:
				locT= new locateT(size,new apex())
				{
					private DoubleWritableRowKey d = new DoubleWritableRowKey();
					private DoubleWritable w = new DoubleWritable();
					private ImmutableBytesWritable buffer = new ImmutableBytesWritable();
					
				    public void add(String combID, double value) 
					{
						int size=PQ.size();
						if(size<T)
							PQ.offer(new gyan(combID, value));
						else
						{
							if(PQ.peek().value<value)
							{
								PQ.poll();
								PQ.offer(new gyan(combID,value));
							}
						}
					}

				    public ArrayList<gyan> asList()
					{
						ArrayList<gyan> n = new ArrayList<gyan>();
						gyan g;
						try
						{
							while(!PQ.isEmpty())
							{
								g=PQ.poll();w.set(g.value);
								g.orderedB=new byte[d.getSerializedLength(w)];
								buffer.set(g.orderedB);d.serialize(w, buffer);
								n.add(g);
							}
						}
						catch(IOException ioex)
						{
							ioex.printStackTrace();
						}
						return n;
					}
				    protected void setOrder() 
				    {
				    	d.setOrder(Order.DESCENDING);
					}
				};
				locT.setOrder();
				return locT;
			case bottom:
				locT =new locateT(size,new nadir())
				{
					private DoubleWritableRowKey d = new DoubleWritableRowKey();
					private DoubleWritable w = new DoubleWritable();
					private ImmutableBytesWritable buffer = new ImmutableBytesWritable();
				    
				    @Override
					public void add(String combID, double value) 
					{
						int size=PQ.size();
						if(size<T)
							PQ.offer(new gyan(combID, value));
						else
						{
							
							if(PQ.peek().value>value)
							{
								PQ.poll();
								PQ.offer(new gyan(combID,value));
							}
						}
					}
				    public ArrayList<gyan> asList()
					{
						ArrayList<gyan> n = new ArrayList<gyan>();
						gyan g;
						try
						{
							while(!PQ.isEmpty())
							{
								g=PQ.poll();w.set(g.value);
								g.orderedB=new byte[d.getSerializedLength(w)];
								buffer.set(g.orderedB);d.serialize(w, buffer);
								n.add(g);
							}
						}
						catch(IOException ioex)
						{
							ioex.printStackTrace();
						}
						return n;
					}
				    protected void setOrder() 
				    {
				    	d.setOrder(Order.ASCENDING);
					}
				    
				};
				locT.setOrder();
				return locT;
		}
		return locT;
	}
}

class apex implements Comparator<gyan>
{
	public int compare(gyan g1,gyan g2)
	{
        if(g1.value<g2.value)return -1;
        else if(g1.value>g2.value)return 1;
        else return 0;
	}
}

class nadir implements Comparator<gyan>
{
	public int compare(gyan g1,gyan g2)
	{
        if(g1.value<g2.value)return 1;
        else if(g1.value>g2.value)return -1;
        else return 0;
	}
}