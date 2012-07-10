
package eu.stratosphere.fuzzyjoin.join;

import java.util.Iterator;
import java.util.UUID;

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;



/**
 * @author Jasir, Arif
 */
public class OnlineAggregationJoin implements PlanAssembler, PlanAssemblerDescription {


	/**
	 */
	public static class LineInFormat extends TextInputFormat<PactNull, PactString> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean readLine(KeyValuePair<PactNull, PactString> pair, byte[] line) {
			pair.setKey(new PactNull());
			pair.setValue(new PactString(new String(line)));
			return true;
		}

	}

	/**
	 */
	public static class OnlineAggregationOutFormat extends TextOutputFormat<PactString, WordAndSumKey> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public byte[] writeLine(KeyValuePair<PactString, WordAndSumKey> pair) {
			String key = pair.getKey().toString();
			String value = pair.getValue().toString();
			String line = key + " " + value + "\n";
			return line.getBytes();
		}

	}

	/**
	 */
	public static class OnlineAggregationMapper extends MapStub<PactString, PactInteger, PactInteger, WordAndSumKey> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void map(PactString key, PactInteger value, Collector<PactInteger, WordAndSumKey> out) {

			String k = key.toString();
			int v = value.getValue();
			
			WordAndSumKey wordAndSumKey = new WordAndSumKey(k, v);
			WordAndSumKey wordAndSumKey2 = new WordAndSumKey(k, v);
			
			PactInteger pactIntegerZero = new PactInteger(0);
			PactInteger pactIntegerOne = new PactInteger(1);
			
			out.collect(pactIntegerZero, wordAndSumKey);
			out.collect(pactIntegerOne, wordAndSumKey2);
			
		
		}

	}

	/**
	 */
	@SameKey
	@Combinable
	public static class OnlineAggregationReducer extends CoGroupStub<PactInteger,PactInteger, WordAndSumKey, PactString, WordAndSumKey> {
		int sum = 0;

		@Override
		public void coGroup(PactInteger key, Iterator<PactInteger> values1,
				Iterator<WordAndSumKey> values2,
				Collector<PactString, WordAndSumKey> out) {

			UUID idOne = UUID.randomUUID();
			long id = idOne.getLeastSignificantBits();
			
    		String keyRecord = "";
    		int sumRecord = 0;
    		WordAndSumKey temp = new WordAndSumKey();
			if (key.getValue() == 0) {
				while (values2.hasNext()) {
					temp = values2.next();
					sum += temp.getSum();
				}
			}
			
			
			if (key.getValue() == 1) {
				while (values2.hasNext()) {

					temp = values2.next();
					keyRecord = temp.getKey();
					sumRecord = temp.getSum();
					WordAndSumKey wordAndSumKey2 = new WordAndSumKey(keyRecord, sumRecord,sum,id);
	    		    out.collect(new PactString(keyRecord), wordAndSumKey2);

				}
			}

		}
		
	}

	/**
	 * {@inheritDoc}
	 */
	public Plan getPlan(String... args) {
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	public String getDescription() {
		return "Parameters: [noSubStasks] [input] [output]";
	}

}
