package bharati.binita.storm.trident.eg1;

import java.io.Serializable;

/**
 * 
 * @author bbharati
 *
 */

public class TupleDS implements Serializable{
	
	private String id;
	private String name;
	private int field1;
	private int field2;
	
	public TupleDS(String id, String name, int field1, int field2) {
		super();
		this.id = id;
		this.name = name;
		this.field1 = field1;
		this.field2 = field2;
	}
	
	
	
	public String getId() {
		return id;
	}



	public void setId(String id) {
		this.id = id;
	}



	public String getName() {
		return name;
	}



	public void setName(String name) {
		this.name = name;
	}



	public int getField1() {
		return field1;
	}



	public void setField1(int field1) {
		this.field1 = field1;
	}



	public int getField2() {
		return field2;
	}



	public void setField2(int field2) {
		this.field2 = field2;
	}



	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "id = "+id+", field1 = "+field1+", field2 = "+field2;
	}

}
