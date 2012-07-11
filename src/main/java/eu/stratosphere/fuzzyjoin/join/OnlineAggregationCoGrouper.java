package eu.stratosphere.fuzzyjoin.join;

import java.util.Iterator;
import java.util.UUID;

import eu.stratosphere.fuzzyjoin.util.WordAndSumKey;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;


 public class OnlineAggregationCoGrouper extends CoGroupStub<PactInteger,PactInteger, WordAndSumKey, PactString, WordAndSumKey> {
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