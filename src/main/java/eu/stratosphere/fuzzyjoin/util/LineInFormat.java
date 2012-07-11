package eu.stratosphere.fuzzyjoin.util;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

	/**
	 * 
	 * Converts a input string (a line) into a KeyValuePair with the string
	 * being the key and the value being a zero Integer.
	 */
	public class LineInFormat extends TextInputFormat<PactNull, PactString> {

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