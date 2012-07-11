package eu.stratosphere.fuzzyjoin.similarity;

import eu.stratosphere.fuzzyjoin.util.WordAndSumKey;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactString;

	@SameKey
	public class SimilarityMatcher extends MatchStub<PactString, WordAndSumKey, WordAndSumKey, PactString, WordAndSumKey> {

		@Override
		public void match(PactString key, WordAndSumKey value1,
				WordAndSumKey value2, Collector<PactString, WordAndSumKey> out) {

			WordAndSumKey wordAndSumKey = new WordAndSumKey(key.getValue(),
					value1.getSum(), value2.getSum(), value1.getUniSum(),
					value2.getUniSum(), value1.getMultisetId(),
					value2.getMultisetId());

			out.collect(key, wordAndSumKey);

		}
	}