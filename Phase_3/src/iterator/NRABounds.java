package iterator;

import java.util.Comparator;

public class NRABounds {

	public float lVal1;
	public float lVal2;
	
	public float uVal1;
	public float uVal2;
	
	public boolean isFullySeen = false;

	public String createBy; //REL1 or REL2

	public NRABounds(float val, String createBy) {
		this.lVal1 = val;
		this.uVal1 = val;	
		this.lVal2 = 0;
		this.uVal2 = 1;
		this.createBy = createBy;
	}
	
	public void updateBounds(float newVal) {
		this.lVal2 = newVal;
		this.uVal2 = newVal;
		this.isFullySeen = true;
	}
	
	public float getLowerBoundVal() {
		return lVal1 + lVal2;
	}
	
	public float getUpperBoundVal() {
		return uVal1 + uVal2;
	}
	
	public String toString() {
		String res = "";
		
		res += "LOW: (" + (lVal1 + lVal2) + ")" + "UPPER: (" + (uVal1  + uVal2) + ")";
		return res;
	}
	
}

class TupleComparator implements Comparator<NRABounds>{
 
    public int compare(NRABounds s1, NRABounds s2) {
        if (s1.getLowerBoundVal() < s2.getLowerBoundVal())
            return 1;
        else if (s1.getLowerBoundVal() > s2.getLowerBoundVal())
            return -1;
                       
        return 0;
     }
}
