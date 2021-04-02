package hashindex;

import java.io.IOException;

import global.Convert;

public class HashKey {

	public static final byte INT = 1;
	public static final byte FLOAT = 2;
	public static final byte STRING = 3;

	byte type;
	int size;
	Object value;

	public HashKey(Integer value) throws IOException {
		this.type = INT;
		this.value = value;
		this.size = HashUtils.getKeyLength(this);
	}

	public HashKey(Float value) throws IOException {
		this.type = FLOAT;
		this.value = value;
		this.size = HashUtils.getKeyLength(this);
	}

	public HashKey(String value) throws IOException {
		this.type = STRING;
		this.value = value;
		this.size = HashUtils.getKeyLength(this);
	}

	public HashKey(HashKey key) {

		this.type = key.type;
		this.size = key.size;

		switch (key.type) {
		case INT:
			this.value = new Integer((Integer) key.value);
			break;
		case FLOAT:
			this.value = new Float((Float) key.value);
			break;
		case STRING:
			this.value = new String((String) key.value);
			break;
		}

	} 

	public HashKey(byte[] data, int offset) throws IOException {

		type = data[offset];
		size = Convert.getIntValue(offset + 1, data);

		switch (type) {
		case INT:
			value = new Integer(Convert.getIntValue(offset + 5, data));
			break;
		case FLOAT:
			value = new Float(Convert.getFloValue(offset + 5, data));
			break;
		case STRING:
			value = Convert.getStrValue(offset + 5, data, size);
			break;
		}

	} 

	public void writeToByteArray(byte[] data, int offset) throws IOException {

		data[offset] = type;
		Convert.setIntValue(size, offset + 1, data);

		switch (type) {
		case INT:
			Convert.setIntValue((Integer) value, offset + 5, data);
			break;
		case FLOAT:
			Convert.setFloValue((Float) value, offset + 5, data);
			break;
		case STRING:
			Convert.setStrValue((String) value, offset + 5, data);
			break;
		}

	}

	public int size() {
		return 1 + 4 + size;
	}


	public int getHash(int depth) {

		// apply the appropriate calculation
		int mask = (1 << depth) - 1;
		switch (type) {

		default:
		case INT:
			int ikey = ((Integer) value).intValue();
			return ikey & mask;

		case FLOAT:
			int fkey = Float.floatToIntBits((Float) value);
			return fkey & mask;

		case STRING:

			// reverse the first four bytes of the string
			byte[] s = ((String) value).getBytes();
			int skey = 0;
			int len = s.length > 4 ? 4 : s.length;
			for (int i = 0; i < len; i++) {
				skey |= (s[i] << (i * Byte.SIZE));
			}
			return skey & mask;

		} // switch

	}

	/**
	 * Returns true if the search key matches the given hash value, false otherwise.
	 */
	public boolean isHash(int hash) {

		// calculate the bit depth (i.e. the left-most '1' bit)
		int depth = (int) (Math.log(hash) / Math.log(2) + 1);

		// compare the hash codes
		return (getHash(depth) == hash);

	} // public boolean isHash(int hash)

	// --------------------------------------------------------------------------

	/**
	 * Returns a generic hash code for the key value.
	 */
	public int hashCode() {
		return value.hashCode();
	}

	/**
	 * True if obj is a SearchKey with the same values; false otherwise.
	 */
	public boolean equals(Object obj) {
		if (obj instanceof HashKey) {
			HashKey key = (HashKey) obj;
			return (value.equals(key.value));
		}
		return false;
	}

	/**
	 * Generically compares two search keys.
	 * 
	 * @return a negative integer, zero, or a positive integer as this object is
	 *         less than, equal to, or greater than the specified object
	 * @throws IllegalArgumentException if the search keys are not comparable
	 */
	public int compareTo(HashKey key) {

		// Integer comparison
		if (value instanceof Integer) {
			if (key.value instanceof Integer) {

				Integer ikey1 = (Integer) this.value;
				Integer ikey2 = (Integer) key.value;
				return ikey1.compareTo(ikey2);

			} else {
				throw new IllegalArgumentException("search keys are not comparable");
			}
		}

		// Float comparison
		if (value instanceof Float) {
			if (key.value instanceof Float) {

				Float fkey1 = (Float) this.value;
				Float fkey2 = (Float) key.value;
				return fkey1.compareTo(fkey2);

			} else {
				throw new IllegalArgumentException("search keys are not comparable");
			}
		}

		// default: String comparison
		if (key.value instanceof String) {

			String skey1 = (String) this.value;
			String skey2 = (String) key.value;
			return skey1.compareTo(skey2);

		} else {
			throw new IllegalArgumentException("search keys are not comparable");
		}

	}

	@Override
	public String toString() {
		return "HashKey [type=" + type + ", size=" + size + ", value=" + value + "]";
	}

}
