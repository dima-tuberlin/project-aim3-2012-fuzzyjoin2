
package eu.stratosphere.fuzzyjoin.similarity;

import java.util.Iterator;

import eu.stratosphere.fuzzyjoin.util.Config;
import eu.stratosphere.fuzzyjoin.util.WordAndSumKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

		
public class SimilarityReducer extends ReduceStub<PactString, WordAndSumKey, PactDouble, PactString> {

			double sum = 0.0;
			double maxSum = 0.0;
			int counter = 0;


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
					double max = Math.max(firstSum, secondSum);
					sum += min;
					maxSum += max;

				}
				
				// ruzicka
//				double tempRuzicka = maxSum;
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
						new PactString("##### Ruzicka Algorithm #### Key: "
								+ wordAndSumKey.getKey() + "    	"
								+ wordAndSumKey.getSum() + Config.SLASH
								+ wordAndSumKey.getUniSum() + " ### "
								+ wordAndSumKey.getSumOfSecondMulti() + Config.SLASH
								+ wordAndSumKey.getUniSumOfSecond()));
				out.collect(
						cosinePact,
						new PactString("##### Cosine Algorithm  #### Key: "
								+ wordAndSumKey.getKey() + "  		 "
								+ wordAndSumKey.getSum() + Config.SLASH
								+ wordAndSumKey.getUniSum() + " ### "
								+ wordAndSumKey.getSumOfSecondMulti() + Config.SLASH
								+ wordAndSumKey.getUniSumOfSecond()));
				out.collect(
						dicePact,
						new PactString("##### Dice Algorithm    #### Key: "
								+ wordAndSumKey.getKey() + "    	"
								+ wordAndSumKey.getSum() + Config.SLASH
								+ wordAndSumKey.getUniSum() + " ### "
								+ wordAndSumKey.getSumOfSecondMulti() + Config.SLASH
								+ wordAndSumKey.getUniSumOfSecond() + "\n"));
				
			}
			

}







