
package hadoopyxd.hadoopyxd.mapreduce;  

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
public class GroupingComparator extends WritableComparator{
	public GroupingComparator() {
		// TODO Auto-generated constructor stub
		 //默认偏移量排序  这里采用自定义排序
		super(SecondSortClass.class, true);
	}
	
	/**
	 * 实现两个对象key之间的比较
	 */
	@Override
	public int compare(Object a, Object b) {
		// TODO Auto-generated method stub
		int re = 0; 
		SecondSortClass a1 = (SecondSortClass) a;
		SecondSortClass b1 = (SecondSortClass) b;

		//比较第一个元素
		re = a1.getFirst().compareTo(b1.getFirst());
		
		return re;
	}
}
  