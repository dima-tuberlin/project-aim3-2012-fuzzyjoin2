/**
 * Copyright (C) 2011 AIM III course DIMA TU Berlin
 *
 * This programm is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.stratosphere.fuzzyjoin.test;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.example.wordcount.WordCount;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class TestA extends TestBase {
	

	public TestA(Configuration config){
		super(config);
	}

	private static final Log LOG = LogFactory.getLog(TestA.class);

	private final String TEXT = "Goethe - Faust: Der Tragoedie erster Teil\n" + "Prolog im Himmel.\n"
			+ "Der Herr. Die himmlischen Heerscharen. Nachher Mephistopheles. Die drei\n" + "Erzengel treten vor.\n"
			+ "RAPHAEL: Die Sonne toent, nach alter Weise, In Brudersphaeren Wettgesang,\n"
			+ "Und ihre vorgeschriebne Reise Vollendet sie mit Donnergang. Ihr Anblick\n"
			+ "gibt den Engeln Staerke, Wenn keiner Sie ergruenden mag; die unbegreiflich\n"
			+ "hohen Werke Sind herrlich wie am ersten Tag.\n"
			+ "GABRIEL: Und schnell und unbegreiflich schnelle Dreht sich umher der Erde\n"
			+ "Pracht; Es wechselt Paradieseshelle Mit tiefer, schauervoller Nacht. Es\n"
			+ "schaeumt das Meer in breiten Fluessen Am tiefen Grund der Felsen auf, Und\n"
			+ "Fels und Meer wird fortgerissen Im ewig schnellem Sphaerenlauf.\n"
			+ "MICHAEL: Und Stuerme brausen um die Wette Vom Meer aufs Land, vom Land\n"
			+ "aufs Meer, und bilden wuetend eine Kette Der tiefsten Wirkung rings umher.\n"
			+ "Da flammt ein blitzendes Verheeren Dem Pfade vor des Donnerschlags. Doch\n"
			+ "deine Boten, Herr, verehren Das sanfte Wandeln deines Tags.\n"
			+ "ZU DREI: Der Anblick gibt den Engeln Staerke, Da keiner dich ergruenden\n"
			+ "mag, Und alle deine hohen Werke Sind herrlich wie am ersten Tag.\n"
			+ "MEPHISTOPHELES: Da du, o Herr, dich einmal wieder nahst Und fragst, wie\n"
			+ "alles sich bei uns befinde, Und du mich sonst gewoehnlich gerne sahst, So\n"
			+ "siehst du mich auch unter dem Gesinde. Verzeih, ich kann nicht hohe Worte\n"
			+ "machen, Und wenn mich auch der ganze Kreis verhoehnt; Mein Pathos braechte\n"
			+ "dich gewiss zum Lachen, Haettst du dir nicht das Lachen abgewoehnt. Von\n"
			+ "Sonn' und Welten weiss ich nichts zu sagen, Ich sehe nur, wie sich die\n"
			+ "Menschen plagen. Der kleine Gott der Welt bleibt stets von gleichem\n"
			+ "Schlag, Und ist so wunderlich als wie am ersten Tag. Ein wenig besser\n"
			+ "wuerd er leben, Haettst du ihm nicht den Schein des Himmelslichts gegeben;\n"
			+ "Er nennt's Vernunft und braucht's allein, Nur tierischer als jedes Tier\n"
			+ "zu sein. Er scheint mir, mit Verlaub von euer Gnaden, Wie eine der\n"
			+ "langbeinigen Zikaden, Die immer fliegt und fliegend springt Und gleich im\n"
			+ "Gras ihr altes Liedchen singt; Und laeg er nur noch immer in dem Grase! In\n"
			+ "jeden Quark begraebt er seine Nase.\n"
			+ "DER HERR: Hast du mir weiter nichts zu sagen? Kommst du nur immer\n"
			+ "anzuklagen? Ist auf der Erde ewig dir nichts recht?\n"
			+ "MEPHISTOPHELES: Nein Herr! ich find es dort, wie immer, herzlich\n"
			+ "schlecht. Die Menschen dauern mich in ihren Jammertagen, Ich mag sogar\n"
			+ "die armen selbst nicht plagen.\n" + "DER HERR: Kennst du den Faust?\n" + "MEPHISTOPHELES: Den Doktor?\n"
			+ "DER HERR: Meinen Knecht!\n"
			+ "MEPHISTOPHELES: Fuerwahr! er dient Euch auf besondre Weise. Nicht irdisch\n"
			+ "ist des Toren Trank noch Speise. Ihn treibt die Gaerung in die Ferne, Er\n"
			+ "ist sich seiner Tollheit halb bewusst; Vom Himmel fordert er die schoensten\n"
			+ "Sterne Und von der Erde jede hoechste Lust, Und alle Naeh und alle Ferne\n"
			+ "Befriedigt nicht die tiefbewegte Brust.\n"
			+ "DER HERR: Wenn er mir auch nur verworren dient, So werd ich ihn bald in\n"
			+ "die Klarheit fuehren. Weiss doch der Gaertner, wenn das Baeumchen gruent, Das\n"
			+ "Bluet und Frucht die kuenft'gen Jahre zieren.\n"
			+ "MEPHISTOPHELES: Was wettet Ihr? den sollt Ihr noch verlieren! Wenn Ihr\n"
			+ "mir die Erlaubnis gebt, Ihn meine Strasse sacht zu fuehren.\n"
			+ "DER HERR: Solang er auf der Erde lebt, So lange sei dir's nicht verboten,\n"
			+ "Es irrt der Mensch so lang er strebt.\n"
			+ "MEPHISTOPHELES: Da dank ich Euch; denn mit den Toten Hab ich mich niemals\n"
			+ "gern befangen. Am meisten lieb ich mir die vollen, frischen Wangen. Fuer\n"
			+ "einem Leichnam bin ich nicht zu Haus; Mir geht es wie der Katze mit der Maus.\n"
			+ "DER HERR: Nun gut, es sei dir ueberlassen! Zieh diesen Geist von seinem\n"
			+ "Urquell ab, Und fuehr ihn, kannst du ihn erfassen, Auf deinem Wege mit\n"
			+ "herab, Und steh beschaemt, wenn du bekennen musst: Ein guter Mensch, in\n"
			+ "seinem dunklen Drange, Ist sich des rechten Weges wohl bewusst.\n"
			+ "MEPHISTOPHELES: Schon gut! nur dauert es nicht lange. Mir ist fuer meine\n"
			+ "Wette gar nicht bange. Wenn ich zu meinem Zweck gelange, Erlaubt Ihr mir\n"
			+ "Triumph aus voller Brust. Staub soll er fressen, und mit Lust, Wie meine\n"
			+ "Muhme, die beruehmte Schlange.\n"
			+ "DER HERR: Du darfst auch da nur frei erscheinen; Ich habe deinesgleichen\n"
			+ "nie gehasst. Von allen Geistern, die verneinen, ist mir der Schalk am\n"
			+ "wenigsten zur Last. Des Menschen Taetigkeit kann allzu leicht erschlaffen,\n"
			+ "er liebt sich bald die unbedingte Ruh; Drum geb ich gern ihm den Gesellen\n"
			+ "zu, Der reizt und wirkt und muss als Teufel schaffen. Doch ihr, die echten\n"
			+ "Goettersoehne, Erfreut euch der lebendig reichen Schoene! Das Werdende, das\n"
			+ "ewig wirkt und lebt, Umfass euch mit der Liebe holden Schranken, Und was\n"
			+ "in schwankender Erscheinung schwebt, Befestigt mit dauernden Gedanken!\n"
			+ "(Der Himmel schliesst, die Erzengel verteilen sich.)\n"
			+ "MEPHISTOPHELES (allein): Von Zeit zu Zeit seh ich den Alten gern, Und\n"
			+ "huete mich, mit ihm zu brechen. Es ist gar huebsch von einem grossen Herrn,\n"
			+ "So menschlich mit dem Teufel selbst zu sprechen.";
	
	
	private final String TEXT2 = "Goethe - Faust: Der Tragoedie erster Teil\n" + "Prolog im Himmel.\n"
			+ "Der Herr. Die himmlischen Heerscharen. Nachher Mephistopheles. Die drei\n" + "Erzengel treten vor.\n"
			+ "RAPHAEL: Die Sonne toent, nach alter Weise, In Brudersphaeren Wettgesang,\n"
			+ "Und ihre vorgeschriebne Reise Vollendet sie mit Donnergang. Ihr Anblick\n"
			+ "gibt den Engeln Staerke, Wenn keiner Sie ergruenden mag; die unbegreiflich\n"
			+ "hohen Werke Sind herrlich wie am ersten Tag.\n"
			+ "GABRIEL: Und schnell und unbegreiflich schnelle Dreht sich umher der Erde\n"
			+ "Pracht; Es wechselt Paradieseshelle Mit tiefer, schauervoller Nacht. Es\n"
			+ "schaeumt das Meer in breiten Fluessen Am tiefen Grund der Felsen auf, Und\n"
			+ "Fels und Meer wird fortgerissen Im ewig schnellem Sphaerenlauf.\n"
			+ "MICHAEL: Und Stuerme brausen um die Wette Vom Meer aufs Land, vom Land\n"
			+ "aufs Meer, und bilden wuetend eine Kette Der tiefsten Wirkung rings umher.\n"
			+ "Da flammt ein blitzendes Verheeren Dem Pfade vor des Donnerschlags. Doch\n"
			+ "deine Boten, Herr, verehren Das sanfte Wandeln deines Tags.\n"
			+ "ZU DREI: Der Anblick gibt den Engeln Staerke, Da keiner dich ergruenden\n"
			+ "mag, Und alle deine hohen Werke Sind herrlich wie am ersten Tag.\n"
			+ "MEPHISTOPHELES: Da du, o Herr, dich einmal wieder nahst Und fragst, wie\n"
			+ "alles sich bei uns befinde, Und du mich sonst gewoehnlich gerne sahst, So";

	private String textPath = null;
	private String resultPath = null;



	@Override
	protected void preSubmit() throws Exception {
		textPath = getFilesystemProvider().getTempDirPath() + "/text";
		resultPath = getFilesystemProvider().getTempDirPath() + "/result";
		
		System.out.println(textPath);
		System.out.println(resultPath);

		getFilesystemProvider().createDir(textPath);

		String[] splits = splitInputString(TEXT, '\n', 4);
		int i = 0;
		for (String split : splits) {
			getFilesystemProvider().createFile(textPath + "/part_" + (i++) + ".txt", split);
			LOG.debug("Text Part " + (i - -1) + ":\n>" + split + "<");
		}
		
		String[] splits2 = splitInputString(TEXT2, '\n', 4);
		int j = 0;
		for (String split : splits2) {
			getFilesystemProvider().createFile(textPath + "/part_" + (j++) + ".txt", split);
			LOG.debug("Text Part " + (j - -1) + ":\n>" + split + "<");
		}


	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		WordCount wc = new WordCount();
		Plan plan = wc.getPlan(config.getString("WordCountTest#NoSubtasks", "1"), getFilesystemProvider()
				.getURIPrefix()
				+ textPath, getFilesystemProvider().getURIPrefix() + resultPath);

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);

	}

	@Override
	protected void postSubmit() throws Exception {
		// clean up hdfs
		getFilesystemProvider().delete(textPath, true);
		getFilesystemProvider().delete(resultPath, true);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("WordCountTest#NoSubtasks", 4);
		tConfigs.add(config);

		return toParameterList(tConfigs);
	}

	private String[] splitInputString(String inputString, char splitChar, int noSplits) {

		String splitString = inputString.toString();
		String[] splits = new String[noSplits];
		int partitionSize = (splitString.length() / noSplits) - 2;

		// split data file and copy parts
		for (int i = 0; i < noSplits - 1; i++) {
			int cutPos = splitString.indexOf(splitChar, (partitionSize < splitString.length() ? partitionSize
					: (splitString.length() - 1)));
			splits[i] = splitString.substring(0, cutPos) + "\n";
			splitString = splitString.substring(cutPos + 1);
		}
		splits[noSplits - 1] = splitString;

		return splits;

	}

}
