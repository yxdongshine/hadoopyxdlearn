
package hadoopyxd.hadoopyxd.mapreduce;  

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/** 
 * ClassName:SecondSortClass <br/> 
 * Function: TODO (自定义的key 排序 分组用). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月2日 下午3:11:37 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class SecondSortClass  implements WritableComparable<SecondSortClass>{

	private String first;
	private int second; 
	
	public SecondSortClass(String first,int second) {
		// TODO Auto-generated constructor stub
		this.first = first;
		this.second =second;
	}
	
	public SecondSortClass() {
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

	
	/**
	 * 比较器
	 */
	public int compareTo(SecondSortClass o) {
		// TODO Auto-generated method stub
		return 0;
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
	

	
	
}
  