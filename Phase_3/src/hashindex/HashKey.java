package hashindex;

import java.io.IOException;

import btree.KeyNotMatchException;
import global.AttrType;
import global.Convert;

public class HashKey {

	byte type;
	int size;
	Object value;

	public HashKey(Integer value) throws IOException {
		this.type = (byte) AttrType.attrInteger;
		this.value = value;
		this.size = HashUtils.getKeyLength(this);
	}

	public HashKey(String value) throws IOException {
		this.type = (byte) AttrType.attrString;
		this.value = value;
		this.size = HashUtils.getKeyLength(this);
	}
	
	public HashKey(Float value) throws IOException {
		this.type = (byte) AttrType.attrReal;
		this.value = value;
		this.size = HashUtils.getKeyLength(this);
	}
	public HashKey(HashKey key) {

		this.type = key.type;
		this.size = key.size;

		switch (key.type) {
		case AttrType.attrInteger:
			this.value = new Integer((Integer) key.value);
			break;
		case AttrType.attrReal:
			this.value = new Float((Float) key.value);
			break;
		case AttrType.attrString:
			this.value = new String((String) key.value);
			break;
		
		}

	}

	public HashKey(byte[] data, int offset) throws IOException, KeyNotMatchException {

		type = data[offset];
		size = Convert.getIntValue(offset + 1, data);

		switch (type) {
		case AttrType.attrInteger:
			value = new Integer(Convert.getIntValue(offset + 5, data));
			break;
		case AttrType.attrReal:
			value = new Float(Convert.getFloValue(offset + 5, data));
			break;
		case AttrType.attrString:
			value = Convert.getStrValue(offset + 5, data, size);
			break;
		default:
			throw new KeyNotMatchException("Unknown key type");
		}

	}

	public void writeToByteArray(byte[] data, int offset) throws IOException, KeyNotMatchException {

		data[offset] = type;
		Convert.setIntValue(size, offset + 1, data);

		switch (type) {
		case AttrType.attrInteger:
			Convert.setIntValue((Integer) value, offset + 5, data);
			break;
		case AttrType.attrReal:
			Convert.setFloValue((Float) value, offset + 5, data);
			break;
		case AttrType.attrString:
			Convert.setStrValue((String) value, offset + 5, data);
			break;
		default:
			throw new KeyNotMatchException("Unknown key type");
		}

	}

	public int size() {
		// type + size + value
		return 1 + 4 + size;
	}

	/**
	 * this function just return the last n bytes of the key
	 * @param n
	 * @return
	 */
	public int getHash(int n) {

		int bitMask = (1 << n) - 1;
		// 000011111 
		switch (type) {

		case AttrType.attrInteger:
			int primIntValue = ((Integer) value).intValue();
			return primIntValue & bitMask;

		case AttrType.attrString:

			byte[] s = ((String) value).getBytes();
			int bitsOfStr = 0;
			for (int i = 0; i < s.length; i++) {
				bitsOfStr |= (s[i] << (i * Byte.SIZE));
			}
			return bitsOfStr & bitMask;
			

		case AttrType.attrReal:
			int primFltIntValue = Float.floatToIntBits((Float) value);
			return primFltIntValue & bitMask;

		default:
			throw new IllegalArgumentException("Unknown key type");
		}

	}

	@Override
	public boolean equals(Object hashKey) {
		if (hashKey instanceof HashKey) {
			HashKey key = (HashKey) hashKey;
			//just compare the value, no need for size and length comparison
			return (value.equals(key.value));
		}
		return false;
	}

	@Override
	public String toString() {
		return "HashKey [type=" + type + ", size=" + size + ", value=" + value + "]";
	}

}
