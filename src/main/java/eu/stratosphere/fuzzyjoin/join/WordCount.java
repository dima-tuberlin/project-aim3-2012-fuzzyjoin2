package eu.stratosphere.fuzzyjoin.join;

import java.util.Iterator;
import java.util.StringTokenizer;

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;



public class WordCount {
	/**
	 * Converts a (String,Integer)-KeyValuePair into multiple KeyValuePairs. The
	 * key string is tokenized by spaces. For each token a new
	 * (String,Integer)-KeyValuePair is emitted where the Token is the key and
	 * an Integer(1) is the value.
	 */
	public static class TokenizeLine extends MapStub<PactNull, PactString, PactString, PactInteger> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void map(PactNull key, PactString value, Collector<PactString, PactInteger> out) {

			String line = value.toString();
			String newline = getReplacedLine(line);

			
			StringTokenizer tokenizer = new StringTokenizer(newline);
			while (tokenizer.hasMoreElements()) {
				String element = (String) tokenizer.nextElement();
				out.collect(new PactString(element), new PactInteger(1));
			}
		}
		
		public String getReplacedLine(String line) {
			line = line.toLowerCase();

			// line = line.replaceAll("\\W", " ");

			if (line.contains("-")) {
				line = line.replace("-", " ");
			}
			if (line.contains(".")) {
				line = line.replace(".", " ");
			}
			if (line.contains(",")) {
				line = line.replace(",", " ");
			}
			if (line.contains(":")) {
				line = line.replace(":", " ");
			}
			if (line.contains("#")) {
				line.replace("#", " ");
			}
			if (line.contains("#")) {
				line.replace("#", " ");
			}
			return line;
		}

	}

	/**
	 * Counts the number of values for a given key. Hence, the number of
	 * occurences of a given token (word) is computed and emitted. The key is
	 * not modified, hence a SameKey OutputContract is attached to this class.
	 */
	@SameKey
	@Combinable
	public static class CountWords extends ReduceStub<PactString, PactInteger, PactString, PactInteger> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void reduce(PactString key, Iterator<PactInteger> values, Collector<PactString, PactInteger> out) {
			int sum = 0;
			while (values.hasNext()) {
				PactInteger element = (PactInteger) values.next();
				sum += element.getValue();
			}

			out.collect(key, new PactInteger(sum));

		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void combine(PactString key, Iterator<PactInteger> values, Collector<PactString, PactInteger> out) {

			this.reduce(key, values, out);
		}

	}
}