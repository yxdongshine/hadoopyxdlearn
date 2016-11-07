
package hadoop.yxd.high.mapreducejion;  

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** 
 * ClassName:JoinDataFormat <br/> 
 * Function: TODO (由于是这个过程输出需要到自定义数据  所以不需要实现比较器). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月7日 下午4:42:43 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class JoinDataFormat implements Writable{
    
	/**
	 * 输出key 标识号码  1 表示用户信息 ，2表示订单信息
	 */
	private Long jionDataKey ; 
	/**
	 * 组装成string
	 */
	private String jionData ;
	
	public JoinDataFormat() {
		// TODO Auto-generated constructor stub
		
	}
	
	public JoinDataFormat(Long jionDataKey,String jionData){
		this.jionDataKey  =jionDataKey ;
		this.jionData = jionData;
	}
	
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.jionDataKey  = arg0.readLong();
		this.jionData  = arg0.readUTF();
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeLong(this.jionDataKey);
		arg0.writeUTF(this.jionData);
	}

	public Long getJionDataKey() {
		return jionDataKey;
	}

	public void setJionDataKey(Long jionDataKey) {
		this.jionDataKey = jionDataKey;
	}

	public String getJionData() {
		return jionData;
	}

	public void setJionData(String jionData) {
		this.jionData = jionData;
	}

	
	//复写tostring
	
	@Override
	public String toString() {
		return "JoinDataFormat [jionDataKey=" + jionDataKey + ", jionData="
				+ jionData + ", getJionDataKey()=" + getJionDataKey()
				+ ", getJionData()=" + getJionData() + ", getClass()="
				+ getClass() + ", hashCode()=" + hashCode() + ", toString()="
				+ super.toString() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((jionData == null) ? 0 : jionData.hashCode());
		result = prime * result
				+ ((jionDataKey == null) ? 0 : jionDataKey.hashCode());
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
		JoinDataFormat other = (JoinDataFormat) obj;
		if (jionData == null) {
			if (other.jionData != null)
				return false;
		} else if (!jionData.equals(other.jionData))
			return false;
		if (jionDataKey == null) {
			if (other.jionDataKey != null)
				return false;
		} else if (!jionDataKey.equals(other.jionDataKey))
			return false;
		return true;
	}

	

	
	
}
  