
package hadoopyxd.hadoopyxd.mapreduce;  

import org.apache.hadoop.io.WritableComparator;

/** 
 * ClassName:SortComparator <br/> 
 * Function: TODO (自定义排序对象). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月2日 下午3:18:27 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class SortComparator extends WritableComparator{

	public SortComparator() {
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

		//先比较第一个元素 在比较第二个元素
		if(!a1.getFirst().equals(b1.getFirst())){
			re = a1.getFirst().compareTo(b1.getFirst());
		}else{
			re = -(a1.getSecond() - b1.getSecond());
		}
		
		return re;
	}
}
  