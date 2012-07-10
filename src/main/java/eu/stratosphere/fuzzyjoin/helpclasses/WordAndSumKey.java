package eu.stratosphere.fuzzyjoin.helpclasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public class WordAndSumKey implements Value {

	String key;
	int sum;
	int sumOfSecondMulti;
	int uniSum;
	int uniSumOfSecond;
	long multisetId;
	long multisetIdOfSecond;


	public WordAndSumKey() {
	}
	
	public WordAndSumKey(String key, int sum) {
		this.key = key;
		this.sum = sum;
	}
	

	public WordAndSumKey(String key, int sum, int uniSum, long multisetId) {
		super();
		this.key = key;
		this.sum = sum;
		this.uniSum = uniSum;
		this.multisetId = multisetId;
	}
	
	public WordAndSumKey(String key, int sum, int sumOfSecondMulti, int uniSum,
			int uniSumOfSecond, long multisetId, long multisetIdOfSecond) {
		super();
		this.key = key;
		this.sum = sum;
		this.sumOfSecondMulti = sumOfSecondMulti;
		this.uniSum = uniSum;
		this.uniSumOfSecond = uniSumOfSecond;
		this.multisetId = multisetId;
		this.multisetIdOfSecond = multisetIdOfSecond;
	}

	
	public String getKey() {
		return key;
	}

	public int getSum() {
		return sum;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public void setSum(int sum) {
		this.sum = sum;
	}
	
	public int getUniSum() {
		return uniSum;
	}

	public long getMultisetId() {
		return multisetId;
	}

	public void setUniSum(int uniSum) {
		this.uniSum = uniSum;
	}
	
	public void setMultisetId(long multisetId) {
		this.multisetId = multisetId;
	}

	public int getSumOfSecondMulti() {
		return sumOfSecondMulti;
	}

	public int getUniSumOfSecond() {
		return uniSumOfSecond;
	}

	public long getMultisetIdOfSecond() {
		return multisetIdOfSecond;
	}

	public void setSumOfSecondMulti(int sumOfSecondMulti) {
		this.sumOfSecondMulti = sumOfSecondMulti;
	}

	public void setUniSumOfSecond(int uniSumOfSecond) {
		this.uniSumOfSecond = uniSumOfSecond;
	}

	public void setMultisetIdOfSecond(long multisetIdOfSecond) {
		this.multisetIdOfSecond = multisetIdOfSecond;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(key);
		out.writeInt(sum);
		out.writeInt(sumOfSecondMulti);
		out.writeInt(uniSum);
		out.writeInt(uniSumOfSecond);
		out.writeLong(multisetId);
		out.writeLong(multisetIdOfSecond);
		
		

	}

	public void read(DataInput in) throws IOException {
		key = in.readUTF();
		sum = in.readInt();
		sumOfSecondMulti = in.readInt();
		uniSum = in.readInt();
		uniSumOfSecond = in.readInt();
		multisetId = in.readLong();
		multisetIdOfSecond = in.readLong();

	}

}
