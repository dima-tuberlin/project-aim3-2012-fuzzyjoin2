package eu.stratosphere.fuzzyjoin.util;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

	/**
	 */
	public class SimilarityOutFormat extends TextOutputFormat<PactDouble, PactString> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public byte[] writeLine(KeyValuePair<PactDouble,PactString> pair) {
			String key = pair.getKey().toString();
			String value = pair.getValue().toString();
			String line = key + " " + value + "\n";
			return line.getBytes();
		}

	}