package oz.tez.samples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
//import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import oz.tez.deployment.utils.TezConstants;
import oz.tez.deployment.utils.YarnUtils;

import com.google.common.base.Preconditions;

public class TezWordCount {
	private final static Log logger = LogFactory.getLog(TezWordCount.class);
	static String INPUT = "Input";
	static String OUTPUT = "Output";
	static String TOKENIZER = "Tokenizer";
	static String SUMMATION = "Summation";

	public static void main(String[] args) throws Exception {
		// [NOTE-1] When executing from the command line you may already have a generated JAR file for your project, 
		// so setting this property to false will not generate JAR from the current workspace. This is purely an IDE-dev feature.
		System.setProperty(TezConstants.GENERATE_JAR, "true");
		// [NOTE-2] This will 'always' update the classpath (see NOTE-3 below), thus ensuring that any changes you may have made (code or JAR)
		// are always reflected when submitting Tez JOB
		System.setProperty(TezConstants.UPDATE_CLASSPATH, "true");
		String inputFile = "sample.txt";

		String appName = "tez-wc";
		String outputPath = appName + "_out";
		final DAG dag = DAG.create(appName);
		TezConfiguration tezConfiguration = new TezConfiguration(new YarnConfiguration());
		FileSystem fs = FileSystem.get(tezConfiguration);
		
		// delete output directory (in case it exists from previous run) - OPTIONAL (safe to comment)
		fs.delete(new Path(outputPath), true);

		// copy source file from local file system to HDFS - OPTIONAL (safe to comment if file is already there)
		Path testFile = new Path(inputFile);
		fs.copyFromLocalFile(false, true, new Path(inputFile), testFile);

		System.out.println("STARTING JOB");

		Path inputPath = fs.makeQualified(new Path(inputFile));
		logger.info("Counting words in " + inputPath);
		logger.info("Building local resources");
		/*
		 * [NOTE-3] The line below is the key for transparent classpath management. Classpath is calculated
		 * and provisioned to HDFS returning map of LocalResources which will be later added
		 * to AM (via TezClient) and each Vertex before
		 */
		Map<String, LocalResource> localResources = YarnUtils.createLocalResources(fs, "spark-cp");
		logger.info("Done building local resources");
		
		final TezClient tezClient = TezClient.create("WordCount", tezConfiguration);
		tezClient.addAppMasterLocalFiles(localResources);
		tezClient.start();
		
		logger.info("Generating DAG");
		
		OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
		        .newBuilder(Text.class.getName(), IntWritable.class.getName(),
		            HashPartitioner.class.getName(), null).build();

		DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(tezConfiguration), TextInputFormat.class, inputFile).build();
		
		DataSinkDescriptor dataSink = MROutput.createConfigBuilder(new Configuration(tezConfiguration), TextOutputFormat.class, outputPath).build();
		
		Vertex mapper = Vertex.create(TOKENIZER, ProcessorDescriptor.create(TokenProcessor.class.getName())).addDataSource(INPUT, dataSource);
		mapper.addTaskLocalFiles(localResources);
		dag.addVertex(mapper);
		
		Vertex reducer = Vertex.create(SUMMATION,ProcessorDescriptor.create(SumProcessor.class.getName()), 1)
		        .addDataSink(OUTPUT, dataSink);
		reducer.addTaskLocalFiles(localResources);
		dag.addVertex(reducer);
		
		dag.addEdge(Edge.create(mapper, reducer, edgeConf.createDefaultEdgeProperty()));
		
		logger.info("Done generating DAG");

		logger.info("Waitin for Tez session");
		tezClient.waitTillReady();
		logger.info("Submitting DAG");
		
		long start = System.currentTimeMillis();
		DAGClient dagClient = tezClient.submitDAG(dag);
		DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
		long stop = System.currentTimeMillis();
		float elapsed = ((float)((stop-start)/(1000*60)));
		logger.info("Finished DAG in " + elapsed + " minutes");
	
		if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
			logger.info("DAG diagnostics: " + dagStatus.getDiagnostics());
		}
		
		tezClient.stop();
		
		// The code below will simply read the output file printing sampled result to the console for validation
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(outputPath), false);
		int counter = 0;
		while (iter.hasNext() && counter++ < 20) {
			LocatedFileStatus status = iter.next();
			if (status.isFile()) {
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(fs.open(status.getPath())));
				String line;
				logger.info("Sampled results from " + status.getPath() + ":");
				while ((line = reader.readLine()) != null && counter++ < 20) {
					logger.info(line);
				}
				logger.info(". . . . . .");
			}

		}
	}

	public static class TokenProcessor extends SimpleProcessor {
	    IntWritable count = new IntWritable(1);
	    Text word = new Text();

	    public TokenProcessor(ProcessorContext context) {
	      super(context);
	    }

	    @Override
	    public void run() throws Exception {
	      Preconditions.checkArgument(getInputs().size() == 1);
	      Preconditions.checkArgument(getOutputs().size() == 1);
	      // the recommended approach is to cast the reader/writer to a specific type instead
	      // of casting the input/output. This allows the actual input/output type to be replaced
	      // without affecting the semantic guarantees of the data type that are represented by
	      // the reader and writer.
	      // The inputs/outputs are referenced via the names assigned in the DAG.
	      KeyValueReader kvReader = (KeyValueReader) getInputs().values().iterator().next().getReader();
	      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

	      while (kvReader.next()) {
	        StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
	        while (itr.hasMoreTokens()) {
	          word.set(itr.nextToken());
	          // Count 1 every time a word is observed. Word is the key a 1 is the value          
	          kvWriter.write(word, count);
	        }
	      }
	    }
	  }

	  /*
	   * Example code to write a processor that commits final output to a data sink
	   * The SumProcessor aggregates the sum of individual word counts generated by 
	   * the TokenProcessor.
	   * The SumProcessor is connected to a DataSink. In this case, its an Output that
	   * writes the data via an OutputFormat to a data sink (typically HDFS). Thats why
	   * it derives from SimpleMRProcessor that takes care of handling the necessary 
	   * output commit operations that makes the final output available for consumers.
	   */
	  public static class SumProcessor extends SimpleMRProcessor {
	    public SumProcessor(ProcessorContext context) {
	      super(context);
	    }

	    @Override
	    public void run() throws Exception {
	      Preconditions.checkArgument(getInputs().size() == 1);
	      Preconditions.checkArgument(getOutputs().size() == 1);

	      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();
	      KeyValuesReader kvReader = (KeyValuesReader) getInputs().values().iterator().next().getReader();
	      
	      while (kvReader.next()) {
	        Text word = (Text) kvReader.getCurrentKey();
	        int sum = 0;
	        for (Object value : kvReader.getCurrentValues()) {
	          sum += ((IntWritable) value).get();
	        }
	        kvWriter.write(word, new LongWritable(sum));
	      }
	      // deriving from SimpleMRProcessor takes care of committing the output
	      // It automatically invokes the commit logic for the OutputFormat if necessary.
	    }
	  }
}
