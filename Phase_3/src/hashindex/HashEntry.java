package hashindex;

import java.io.IOException;

import global.Convert;
import global.PageId;
import global.RID;


public class HashEntry {

	public HashKey key;
    public RID rid;
	
    public HashEntry(HashKey searchkey, RID rid1)
    {
        key = new HashKey(searchkey);
        rid = new RID(rid1.pageNo, rid1.slotNo);
    }

    public HashEntry(byte data[], int offset) throws IOException
    {
        key = new HashKey(data, offset);
        rid = getRIDFromByteArr(data, offset + key.getLength());
    }

    public void writeData(byte abyte0[], int offset) throws IOException
    {
        key.writeData(abyte0, offset);
        rid.writeToByteArray(abyte0, offset + key.getLength());
    }

    public int getLength()
    {
        return key.getLength() + 8;//rid length is 8 bytes
    }

    public boolean equals(Object obj)
    {
        if(obj instanceof HashEntry)
        {
        	HashEntry dataentry = (HashEntry)obj;
            return key.compareTo(dataentry.key) == 0 && rid.equals(dataentry.rid);
        } else
        {
            return false;
        }
    }
    
    public static RID getRIDFromByteArr(byte[] bytearr,int offset) throws IOException {
		int slotNo=Convert.getIntValue(offset, bytearr);
		int pageNo=Convert.getIntValue(offset+4, bytearr);
		return new RID(new PageId(pageNo), slotNo);
	}
  

    
}
