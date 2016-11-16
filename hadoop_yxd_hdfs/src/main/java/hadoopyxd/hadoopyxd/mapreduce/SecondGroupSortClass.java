package hadoopyxd.hadoopyxd.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SecondGroupSortClass implements  WritableComparable<SecondGroupSortClass> {

	private String first;
	private int second; 
	
	public SecondGroupSortClass(String first,int second) {
		// TODO Auto-generated constructor stub
		this.first = first;
		this.second =second;
	}
	
	public SecondGroupSortClass() {
		// TODO Auto-generated constructor stub
	}
	//序列化输入流
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.first= arg0.readUTF();
		this.second  =arg0.readInt();
	}
    //序列化输出流
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(first);
		arg0.writeInt(second);
	}



	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public String toString() {
		return "SecondSortClass [first=" + first + ", second=" + second + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondGroupSortClass other = (SecondGroupSortClass) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second != other.second)
			return false;
		return true;
	}

	public int compareTo(SecondGroupSortClass o) {
		// TODO Auto-generated method stub
		int re = 0; 
		SecondGroupSortClass a1 = (SecondGroupSortClass) o;
		SecondGroupSortClass b1 = (SecondGroupSortClass) this;

		//先比较第一个元素 在比较第二个元素
		re = a1.getFirst().compareTo(b1.getFirst());
		
		return re;
	}
	

	
	
	
}

