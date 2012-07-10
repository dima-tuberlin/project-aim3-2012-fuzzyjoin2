package eu.stratosphere.fuzzyjoin.similarity;

import java.util.Iterator;

import eu.stratosphere.fuzzyjoin.join.WordAndSumKey;

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
