
package eu.stratosphere.fuzzyjoin.join;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import eu.stratosphere.fuzzyjoin.helpclasses.Config;
import eu.stratosphere.fuzzyjoin.helpclasses.LineInFormat;
import eu.stratosphere.fuzzyjoin.helpclasses.SimilarityOutFormat;
import eu.stratosphere.fuzzyjoin.helpclasses.WordAndSumKey;
import eu.stratosphere.fuzzyjoin.similarity.SimilarityMatcher;
import eu.stratosphere.fuzzyjoin.similarity.SimilarityRuzicka;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;



/**
 * @author Jasir, Arif
 */
public class WordCount implements PlanAssembler, PlanAssemblerDescription {



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
			line = line.toLowerCase(); 

//			line = line.replaceAll("\\W", " ");			
			
			if(line.contains("-")){
			line = line.replace("-", " ");
			}
			if(line.contains(".")){
			line = line.replace(".", " ");
			}
			if(line.contains(",")){
			line = line.replace(",", " ");
			}
			if(line.contains(":")){
			line = line.replace(":", " ");
			}
//			String [] lineArray =  line.split(":");
//			String id = lineArray[0];
//			String title = lineArray[1];
//			String author = lineArray[2];
//			String publication = lineArray[3];
//
//			String data = id + " " +  title + " " + author + " " + publication ;
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreElements()) {
				String element = (String) tokenizer.nextElement();
				out.collect(new PactString(element), new PactInteger(1));
			}
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
	
	/**
	 * {@inheritDoc}
	 */
	public Plan getPlan(String... args) {

		// parse job parameters
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String dataInput2 = (args.length > 2 ? args[2] : "");
		String output    = (args.length > 3 ? args[3] : "");

		FileDataSourceContract<PactNull, PactString> data = new FileDataSourceContract<PactNull, PactString>(
				LineInFormat.class, dataInput, "First Input Lines");
		data.setDegreeOfParallelism(noSubTasks);
		
		FileDataSourceContract<PactNull, PactString> data2 = new FileDataSourceContract<PactNull, PactString>(
				LineInFormat.class, dataInput2, "Second Input Lines");
		data2.setDegreeOfParallelism(noSubTasks);

		MapContract<PactNull, PactString, PactString, PactInteger> mapper = new MapContract<PactNull, PactString, PactString, PactInteger>(
				TokenizeLine.class, "Tokenize First Lines");
		mapper.setDegreeOfParallelism(noSubTasks);
		
		MapContract<PactNull, PactString, PactString, PactInteger> secondWordCountMapper = new MapContract<PactNull, PactString, PactString, PactInteger>(
				TokenizeLine.class, "Tokenize Second Lines");
		secondWordCountMapper.setDegreeOfParallelism(noSubTasks);

		ReduceContract<PactString, PactInteger, PactString, PactInteger> reducer = new ReduceContract<PactString, PactInteger, PactString, PactInteger>(
				CountWords.class, "First Count Words");
		reducer.setDegreeOfParallelism(noSubTasks);
		
		ReduceContract<PactString, PactInteger, PactString, PactInteger> secondWordCountReducer = new ReduceContract<PactString, PactInteger, PactString, PactInteger>(
				CountWords.class, "Second Count Words");
		secondWordCountReducer.setDegreeOfParallelism(noSubTasks);
		
		MapContract<PactString, PactInteger, PactInteger, WordAndSumKey> onlineMapper = new MapContract<PactString, PactInteger, PactInteger, WordAndSumKey>(
				OnlineAggregationMapper.class, "First OnlineAggregation Mapper");
		onlineMapper.setDegreeOfParallelism(noSubTasks);
		
		MapContract<PactString, PactInteger, PactInteger, WordAndSumKey> secondOnlineMapper = new MapContract<PactString, PactInteger, PactInteger, WordAndSumKey>(
				OnlineAggregationMapper.class, "Second OnlineAggregation Mapper");
		secondOnlineMapper.setDegreeOfParallelism(noSubTasks);

		CoGroupContract<PactInteger,PactInteger, WordAndSumKey, PactString, WordAndSumKey> coGroup = new CoGroupContract<PactInteger,PactInteger, WordAndSumKey, PactString, WordAndSumKey>(
				OnlineAggregationReducer.class, "First OnlineAggregation Reducer");
		coGroup.setDegreeOfParallelism(noSubTasks);
		
		CoGroupContract<PactInteger,PactInteger, WordAndSumKey, PactString, WordAndSumKey> coGroup2 = new CoGroupContract<PactInteger,PactInteger, WordAndSumKey, PactString, WordAndSumKey>(
				OnlineAggregationReducer.class, "Second OnlineAggregation Reducer");
		coGroup2.setDegreeOfParallelism(noSubTasks);

		
		MatchContract<PactString, WordAndSumKey, WordAndSumKey, PactString, WordAndSumKey> matcher = new MatchContract<PactString, WordAndSumKey, WordAndSumKey, PactString, WordAndSumKey>(
				SimilarityMatcher.class, "SimilarityMatcher");
		matcher.setDegreeOfParallelism(noSubTasks);
		
		ReduceContract<PactString, WordAndSumKey, PactDouble, PactString> similarityReducer = new ReduceContract<PactString, WordAndSumKey, PactDouble, PactString>(
				SimilarityRuzicka.class, "SimilarityAlgorithmsReducer");
		similarityReducer.setDegreeOfParallelism(noSubTasks);
		
		
		FileDataSinkContract<PactDouble, PactString> out = new FileDataSinkContract<PactDouble, PactString>(
				SimilarityOutFormat.class, output, "Similarity Output");
		out.setDegreeOfParallelism(noSubTasks);

		
		out.setInput(similarityReducer);
		
		// similarity
		similarityReducer.setInput(matcher);
		matcher.setSecondInput(coGroup2);
		matcher.setFirstInput(coGroup);
		
		/* 
		 * join phase
		 */
		//join
		coGroup2.setSecondInput(secondOnlineMapper);
		coGroup2.setFirstInput(secondOnlineMapper);
		coGroup.setSecondInput(onlineMapper);
		coGroup.setFirstInput(onlineMapper); 
		
		/*
		 * wordcount
		 */
		
		//second
		secondOnlineMapper.setInput(secondWordCountReducer);
		secondWordCountReducer.setInput(secondWordCountMapper);
		
		//first
		onlineMapper.setInput(reducer);
		
		reducer.setInput(mapper);
		
		
		
		//data input
		secondWordCountMapper.setInput(data2);
		mapper.setInput(data);

		

		return new Plan(out, "V-Smart Example");
	}

	/**
	 * {@inheritDoc}
	 */
	public String getDescription() {
		return Config.STRATOSPHERE_DESCRIPTION;
	}

	
//	public static void main(String[] args){
//		BufferedReader reader = null;
//		try {
//			reader = new BufferedReader(new FileReader("./src/main/resources/dblpraw.txt"));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		String line;
//		try {
//			while((line = reader.readLine()) != null){
//			
////				line = line.replaceAll("\\W", " ");			
//
//				if(line.contains("-")){
//				line = line.replace("-", " ");
//				}
//				if(line.contains(".")){
//				line = line.replace(".", " ");
//				}
//				if(line.contains(",")){
//				line = line.replace(",", " ");
//				}
//				if(line.contains(":")){
//				line = line.replace(":", " ");
//				}
//				
//				System.out.println(line);
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
}
