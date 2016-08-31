/*
* Copyright (c) 2016, UChicago Argonne, LLC. All rights reserved.
*
* Copyright 2016. UChicago Argonne, LLC. This software was produced 
* under U.S. Government contract DE-AC02-06CH11357 for Argonne National 
* Laboratory (ANL), which is operated by UChicago Argonne, LLC for the 
* U.S. Department of Energy. The U.S. Government has rights to use, 
* reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR 
* UChicago Argonne, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR 
* ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is 
* modified to produce derivative works, such modified software should 
* be clearly marked, so as not to confuse it with the version available 
* from ANL.

* Additionally, redistribution and use in source and binary forms, with 
* or without modification, are permitted provided that the following 
* conditions are met:
*
*   * Redistributions of source code must retain the above copyright 
*     notice, this list of conditions and the following disclaimer. 
*
*   * Redistributions in binary form must reproduce the above copyright 
*     notice, this list of conditions and the following disclaimer in 
*     the documentation and/or other materials provided with the 
*     distribution. 
*
*   * Neither the name of UChicago Argonne, LLC, Argonne National 
*      Laboratory, ANL, the U.S. Government, nor the names of its 
*      contributors may be used to endorse or promote products derived 
*      from this software without specific prior written permission. 

* THIS SOFTWARE IS PROVIDED BY UChicago Argonne, LLC AND CONTRIBUTORS 
* "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
* LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
* FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL UChicago 
* Argonne, LLC OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
* INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
* BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
* CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
* LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
* ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
* POSSIBILITY OF SUCH DAMAGE.
*/

package gov.anl.aps.xpcs.mapred.job;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import gov.anl.aps.xpcs.config.XPCSConfig;
import gov.anl.aps.xpcs.mapred.G2KineticsReducer;
import gov.anl.aps.xpcs.mapred.G2Reducer;
import gov.anl.aps.xpcs.mapred.IMM2TFileFormat;
import gov.anl.aps.xpcs.mapred.IMMFileFormat;
import gov.anl.aps.xpcs.mapred.TwoTimesReducer;
import gov.anl.aps.xpcs.mapred.io.CompositeKeyComparator;
import gov.anl.aps.xpcs.mapred.io.FrameValue;
import gov.anl.aps.xpcs.mapred.io.G2Key;
import gov.anl.aps.xpcs.mapred.io.G2Value;
import gov.anl.aps.xpcs.mapred.io.NaturalKeyGroupingComparator;
import gov.anl.aps.xpcs.mapred.io.NaturalKeyPartitioner;
import gov.anl.aps.xpcs.mapred.io.PixelKey;
import gov.anl.aps.xpcs.mapred.io.PixelValue;
import gov.anl.aps.xpcs.mapred.io.QMapKey;
import gov.anl.aps.xpcs.mapred.io.QMapValue;
import gov.anl.aps.xpcs.mapred.io.TwoTimeBinaryValue;
import gov.anl.aps.xpcs.mapred.io.TwoTimeOutputFormat;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO Possibly can split G2JobConf and NormJobConf and make some abstract JobConf class. 
public class TwoTimesJobConf extends G2JobConf {
    
    private static final Logger logger = LoggerFactory
            .getLogger(TwoTimesJobConf.class.getName());

    public TwoTimesJobConf(XPCSConfig config) throws IOException {
        super(config, "(TwoTime)-");
        
		 setupSGSymLinks();
		 setupFrameSumSymLinks();

    }
    
    public TwoTimesJobConf(XPCSConfig config, String name) throws IOException {
        super(config, name);

		 setupSGSymLinks();
		 setupFrameSumSymLinks();

    }    

	protected void setMapReduceParams() {
		// Input format
		Path input = new Path(config.getInputFilePath());
		Path output = new Path(config.getOutputDir() + "/g2");

		logger.info("Input path " + input.getName());
		logger.info("Output path " + output.getName());

		// Input/Output paths
		FileInputFormat.setInputPaths(this, input);
		FileOutputFormat.setOutputPath(this, output);

		setInputFormat(IMM2TFileFormat.class);
		setOutputFormat(TwoTimeOutputFormat.class);
        // setOutputFormat(TextOutputFormat.class);
        
		setPartitionerClass(NaturalKeyPartitioner.class); 
		setOutputKeyComparatorClass(CompositeKeyComparator.class);
		setOutputValueGroupingComparator(NaturalKeyGroupingComparator.class);

		// Result output key-value pairs		
		setOutputKeyClass(QMapKey.class);
		setOutputValueClass(TwoTimeBinaryValue.class);
        // setOutputValueClass(QMapValue.class);

		setMapOutputKeyClass(QMapKey.class);
		setMapOutputValueClass(QMapValue.class);

		// Map Reduce functions
		setMapperClass(IdentityMapper.class);
		setReducerClass(TwoTimesReducer.class);
         // setReducerClass(IdentityReducer.class);

         // Set number of reducers to match number of bins
        String bins[] = this.config.getQMapBinsToProcess();
        if (bins != null && bins.length > 0)
            setNumReduceTasks(bins.length);

        this.setInt("dfs.replication", 1);

	}

    protected void setupSGSymLinks() throws IOException {
    	try {
    		DistributedCache.addCacheFile(new URI(
                config.getOutputDir() + "/smoothedSG#smoothedSG"), this);
    	} catch (URISyntaxException e) {
    		e.printStackTrace();
    	}
    }
    
    protected void setupFrameSumSymLinks() throws IOException {
    	try {
    		DistributedCache.addCacheFile(new URI(
                config.getOutputDir() + "/frameSum#frameSum"), this);
    	} catch (URISyntaxException e) {
    		e.printStackTrace();
    	}
    }
}

