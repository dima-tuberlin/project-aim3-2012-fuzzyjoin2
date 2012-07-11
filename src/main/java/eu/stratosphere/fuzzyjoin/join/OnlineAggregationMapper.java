package eu.stratosphere.fuzzyjoin.join;

import eu.stratosphere.fuzzyjoin.util.WordAndSumKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class OnlineAggregationMapper extends MapStub<PactString, PactInteger, PactInteger, WordAndSumKey> {

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