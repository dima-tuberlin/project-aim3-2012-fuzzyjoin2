
package eu.stratosphere.fuzzyjoin.plan;

import eu.stratosphere.fuzzyjoin.join.OnlineAggregationCoGrouper;
import eu.stratosphere.fuzzyjoin.join.OnlineAggregationMapper;
import eu.stratosphere.fuzzyjoin.join.WordCount.CountWords;
import eu.stratosphere.fuzzyjoin.join.WordCount.TokenizeLine;
import eu.stratosphere.fuzzyjoin.similarity.SimilarityMatcher;
import eu.stratosphere.fuzzyjoin.similarity.SimilarityReducer;
import eu.stratosphere.fuzzyjoin.util.Config;
import eu.stratosphere.fuzzyjoin.util.LineInFormat;
import eu.stratosphere.fuzzyjoin.util.SimilarityOutFormat;
import eu.stratosphere.fuzzyjoin.util.WordAndSumKey;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;



/**
 * @author Jasir, Arif
 */
public class SmartFuzzyPlan implements PlanAssembler, PlanAssemblerDescription {

	
	/**
	 * {@inheritDoc}
	 */
	@Override
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
				OnlineAggregationCoGrouper.class, "First OnlineAggregation CoGrouper");
		coGroup.setDegreeOfParallelism(noSubTasks);
		
		CoGroupContract<PactInteger,PactInteger, WordAndSumKey, PactString, WordAndSumKey> coGroup2 = new CoGroupContract<PactInteger,PactInteger, WordAndSumKey, PactString, WordAndSumKey>(
				OnlineAggregationCoGrouper.class, "Second OnlineAggregation CoGrouper");
		coGroup2.setDegreeOfParallelism(noSubTasks);

		
		MatchContract<PactString, WordAndSumKey, WordAndSumKey, PactString, WordAndSumKey> matcher = new MatchContract<PactString, WordAndSumKey, WordAndSumKey, PactString, WordAndSumKey>(
				SimilarityMatcher.class, "SimilarityMatcher");
		matcher.setDegreeOfParallelism(noSubTasks);
		
		ReduceContract<PactString, WordAndSumKey, PactDouble, PactString> similarityReducer = new ReduceContract<PactString, WordAndSumKey, PactDouble, PactString>(
				SimilarityReducer.class, "SimilarityAlgorithmsReducer");
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
	@Override
	public String getDescription() {
		return Config.STRATOSPHERE_DESCRIPTION;
	}

	
}
