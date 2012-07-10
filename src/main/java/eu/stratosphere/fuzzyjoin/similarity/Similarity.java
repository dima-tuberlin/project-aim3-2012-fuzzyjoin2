
package eu.stratosphere.fuzzyjoin.similarity;

import java.util.Iterator;

import eu.stratosphere.fuzzyjoin.helpclasses.Config;
import eu.stratosphere.fuzzyjoin.join.WordAndSumKey;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;



/**
 * @author Jasir, Arif
 */
public class Similarity implements PlanAssembler, PlanAssemblerDescription {


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
	public static class SimilarityOutFormat extends TextOutputFormat<PactDouble, PactString> {

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

	/**
	 */
	
	public static class SimilarityMatcher extends MatchStub<PactString, WordAndSumKey, WordAndSumKey, PactString, WordAndSumKey> {

		@Override
		public void match(PactString key, WordAndSumKey value1,
				WordAndSumKey value2, Collector<PactString, WordAndSumKey> out) {

			WordAndSumKey wordAndSumKey = new WordAndSumKey(key.getValue(),
					value1.getSum(), value2.getSum(), value1.getUniSum(),
					value2.getUniSum(), value1.getMultisetId(),
					value2.getMultisetId());

			out.collect(key, wordAndSumKey);

		}

		
		public static class SimilarityRuzicka extends ReduceStub<PactString, WordAndSumKey, PactDouble, PactString> {

			double sum = 0.0;

			@Override
			public void reduce(PactString key, Iterator<WordAndSumKey> values,
					Collector<PactDouble, PactString> out) {

				WordAndSumKey wordAndSumKey = new WordAndSumKey();
				
			
				double uniSum1 = 0.0;
				double uniSum2 = 0.0;
				while (values.hasNext()) {
					wordAndSumKey = values.next();
					int firstSum = wordAndSumKey.getSum();
					int secondSum = wordAndSumKey.getSumOfSecondMulti();
					uniSum1 = wordAndSumKey.getUniSum();
					uniSum2 = wordAndSumKey.getUniSumOfSecond();
					double min = Math.min(firstSum, secondSum);	
					sum += min;

				}
				
				// ruzicka
				double tempRuzicka = uniSum1 + uniSum2 - sum;
				double resultOfRuzicka = sum / tempRuzicka;
				// cosine
				double tempCosine = Math.sqrt(uniSum1 * uniSum2);
				double resultOfCosine = sum / tempCosine;
				// dice
				double tempDice = uniSum1 + uniSum2;
				double resultOfDice = (2 * sum) / tempDice;
				
				
				PactDouble resultPact = new PactDouble(resultOfRuzicka);
				PactDouble cosinePact = new PactDouble(resultOfCosine);
				PactDouble dicePact = new PactDouble(resultOfDice);
				out.collect(
						resultPact,
						new PactString("Sum:  "+ sum + "##### Ruzicka Algorithm #### Key: "
								+ wordAndSumKey.getKey() + " "
								+ wordAndSumKey.getSum() + Config.SLASH
								+ wordAndSumKey.getUniSum() + " ### "
								+ wordAndSumKey.getSumOfSecondMulti() + Config.SLASH
								+ wordAndSumKey.getUniSumOfSecond()));
				out.collect(
						cosinePact,
						new PactString("##### Cosine Algorithm  #### Key: "
								+ wordAndSumKey.getKey() + " "
								+ wordAndSumKey.getSum() + Config.SLASH
								+ wordAndSumKey.getUniSum() + " ### "
								+ wordAndSumKey.getSumOfSecondMulti() + Config.SLASH
								+ wordAndSumKey.getUniSumOfSecond()));
				out.collect(
						dicePact,
						new PactString("##### Dice Algorithm    #### Key: "
								+ wordAndSumKey.getKey() + " "
								+ wordAndSumKey.getSum() + Config.SLASH
								+ wordAndSumKey.getUniSum() + " ### "
								+ wordAndSumKey.getSumOfSecondMulti() + Config.SLASH
								+ wordAndSumKey.getUniSumOfSecond() + "\n"));
				
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
