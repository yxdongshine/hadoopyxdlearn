
package hadoopyxd.hadoopyxd.mapreduce;  

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/** 
 * ClassName:GroupingComparator <br/> 
 * Function: TODO (reduce段自定义分组). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月2日 下午3:25:54 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class GroupingComparator implements RawComparator<SecondGroupSortClass>{
	public GroupingComparator() {
		// TODO Auto-generated constructor stub
		 //默认偏移量排序  这里采用自定义排序
		super();
	}
	
	/**
	 * 实现两个对象key之间的比较
	 */
	public int compare(SecondGroupSortClass a, SecondGroupSortClass b) {
		// TODO Auto-generated method stub
		int re = 0; 
		SecondGroupSortClass a1 = (SecondGroupSortClass) a;
		SecondGroupSortClass b1 = (SecondGroupSortClass) b;

		//比较第一个元素
		re = a1.getFirst().compareTo(b1.getFirst());
		
		return re;
	}

	/** 
     * @param arg0 表示第一个参与比较的字节数组 
     * @param arg1 表示第一个参与比较的字节数组的起始位置 
     * @param arg2 表示第一个参与比较的字节数组的偏移量 
     *  
     * @param arg3 表示第二个参与比较的字节数组 
     * @param arg4 表示第二个参与比较的字节数组的起始位置 
     * @param arg5 表示第二个参与比较的字节数组的偏移量 
     */  
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,  
            int arg4, int arg5) {  
        return WritableComparator.compareBytes(arg0, arg1, 8, arg3, arg4, 8);  
    }  



}
  