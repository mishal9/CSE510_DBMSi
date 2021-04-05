package hashindex;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import global.AttrType;

public class HashUtils {

	public final static int getKeyLength(HashKey key) throws IOException {
		switch (key.type) {
		case AttrType.attrInteger:
			return 4;
		case AttrType.attrReal:
			return 4;
		case AttrType.attrString:
			OutputStream out = new ByteArrayOutputStream();
			DataOutputStream outstr = new DataOutputStream(out);
			outstr.writeUTF(((String) key.value));
			return outstr.size();
		default:
			throw new IOException("key types do not match");
		}

	}

	public static byte[] intToBytes(int x) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bos);
		out.writeInt(x);
		out.close();
		byte[] int_bytes = bos.toByteArray();
		bos.close();
		return int_bytes;
	}
	
	//util function to debug log stuff
	// if no debug log wanted, comment the print statement
	public static void log(Object str) {
		System.out.println(str);
	}
	
}
