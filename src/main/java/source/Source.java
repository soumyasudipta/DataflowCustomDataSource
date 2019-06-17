
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import source.Socket;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;


public class Source extends UnboundedSource<String, UnboundedSource.CheckpointMark>{
	
	  public String URI= "";
	  Source(String URI){
		  this.URI = URI; 
	  }
	
	  private static final long serialVersionUID = 1L;
	  private static final Logger LOG = LoggerFactory.getLogger(Source.class);
	  
	  @Override
	  public List<? extends UnboundedSource<String, CheckpointMark>> split(int desiredNumSplits, PipelineOptions options)
			throws Exception {
		// TODO Auto-generated method stub
		return Arrays.asList(this);
	  }

	  @Override
	  public UnboundedReader<String> createReader(
	      PipelineOptions pipelineOptions, CheckpointMark checkpointMark) {
	    return new Socket(this,URI);
	  }

	  @Nullable
	  @Override
	  public Coder<CheckpointMark> getCheckpointMarkCoder() {
	    return null;
	  }

	  @Override
	  public StructuredCoder<String> getDefaultOutputCoder() {
	    return StringUtf8Coder.of();
	  }
	  
	  public static void main(String[] args) {
		  
		  LOG.info("Pipeline Started");
		
		  PipelineOptions options =
			       PipelineOptionsFactory.fromArgs(args).create();
		
		  Pipeline pipeline = Pipeline.create(options);
		  
		  
		  PCollection<String> ws1 = pipeline.apply(Read.from(new Source(Config.getURI()[0])));
		  PCollection<String> ws2 = pipeline.apply(Read.from(new Source(Config.getURI()[1])));
		  PCollection<String> ws3 = pipeline.apply(Read.from(new Source(Config.getURI()[2])));
		  PCollection<String> ws4 = pipeline.apply(Read.from(new Source(Config.getURI()[3])));
		  PCollection<String> ws5 = pipeline.apply(Read.from(new Source(Config.getURI()[4])));
		  PCollection<String> ws6 = pipeline.apply(Read.from(new Source(Config.getURI()[5])));
		  
		  PCollectionList<String> pcs = PCollectionList.of(ws1).and(ws2).and(ws3).and(ws4).and(ws5).and(ws6);
		  pcs.apply(Flatten.<String>pCollections()).apply("Print Data",ParDo.of(new PrintData()));
		  
		  pipeline.run();    
	  }
	  
	  public static class PrintData extends DoFn<String, String> {

		    /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@ProcessElement
		    public void processElement(ProcessContext c) throws Exception {
		      System.out.println(c.element());
		    }
		  }
}