package eu.stratosphere.fuzzyjoin.helpclasses;

import java.util.Iterator;


/**
 * 
 * @author jasir
 *
 */
public class AnalyticFunctions {

	public int getSum(Iterator<WordAndSumKey> values2) {
		int sum = 0;
		while (values2.hasNext()) {
			sum += values2.next().getSum();
		}
		return sum;
	}

}
