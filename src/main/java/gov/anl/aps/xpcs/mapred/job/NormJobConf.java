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
import gov.anl.aps.xpcs.mapred.PartitionMapper;
import gov.anl.aps.xpcs.mapred.PartitionReducer;
import gov.anl.aps.xpcs.mapred.io.NormResult;
import gov.anl.aps.xpcs.mapred.io.PartitionKey;
import gov.anl.aps.xpcs.mapred.io.PartitionValue;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NormJobConf extends JobConf {
    
    private static final Logger logger = LoggerFactory
            .getLogger(NormJobConf.class.getName());
    
    private String jobName = "(norm)-";
    private XPCSConfig config = null;
    
    //TODO derive this class from a generic (G2Job) job class
    public NormJobConf(XPCSConfig config) throws IOException {
        super(config);
        this.config = config;
        
        init();
    }
    
    public NormJobConf(XPCSConfig config, String name) throws IOException {
        super(config);
        this.jobName = name;
        this.config = config;
        
        init();
    }
    
    private void init() throws IOException {
        try {
            String file = this.config.getHDF5ConfigFile();
            File f = new File(file);
            jobName = jobName + f.getName();
        } catch (Exception e) {} 
 
        setJobName(jobName);
        
        logger.info("Initilaizing G2Job configuration");
        
        setMapReduceParams();
        setupPartitions();
       
    }
    
    private void setMapReduceParams() {
        // Input format
        //TODO: Move prefix of output directories to configuration
        Path input = new Path(config.getOutputDir() + "/g2");
        Path output = new Path(config.getOutputDir() + "/norm");
        logger.info("Input path " + input.getName());
        logger.info("Output path " + output.getName());
        
        // Input/Output paths
        FileInputFormat.setInputPaths(this, input);
        FileInputFormat.setInputPathFilter(this, NormJobFilter.class);
        FileOutputFormat.setOutputPath(this, output);
        
        // Input/Output formats
        setInputFormat(SequenceFileInputFormat.class);
        setOutputFormat(TextOutputFormat.class);
        
        // Result output key-value pairs
        setOutputKeyClass(PartitionKey.class);
        setOutputValueClass(NormResult.class);
        
        // Map output key-value pairs
        setMapOutputKeyClass(PartitionKey.class);
        setMapOutputValueClass(PartitionValue.class);
        
        // Map Reduce functions
        setMapperClass(PartitionMapper.class);
        setReducerClass(PartitionReducer.class);

    }
    
    private void setupPartitions() throws IOException {
        try {
            //TODO: Generate the qmap file with unique time-stamp to avoid any
            // conflicts in future with the name.
            DistributedCache.addCacheFile(new URI(
                config.getOutputDir() + "/dqmap" + "#dqmap"), 
                    this);
            DistributedCache.addCacheFile(new URI(
                config.getOutputDir() + "/sqmap" + "#sqmap"), 
                    this);
            DistributedCache.createSymlink(this);

        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
}

