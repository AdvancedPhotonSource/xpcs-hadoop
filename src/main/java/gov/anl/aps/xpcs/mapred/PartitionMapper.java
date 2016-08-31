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

package gov.anl.aps.xpcs.mapred;

import gov.anl.aps.xpcs.config.XPCSConfig;
import gov.anl.aps.xpcs.mapred.io.G2Key;
import gov.anl.aps.xpcs.mapred.io.G2Value;
import gov.anl.aps.xpcs.mapred.io.PartitionKey;
import gov.anl.aps.xpcs.mapred.io.PartitionValue;
import gov.anl.aps.xpcs.util.QMaps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import javax.management.RuntimeErrorException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Maps G2,IF,IP values based on their dynamic partition.
 * 
 * @author mmccuiston
 * 
 */
public class PartitionMapper extends MapReduceBase
        implements Mapper<G2Key, G2Value, PartitionKey, PartitionValue> {

    /**
     * Contains dynamic partition id and tau value
     */
    private PartitionKey partitionKey = new PartitionKey();
    
    /**
     * Contains g2,ipast,ifture,static-partition-id for the given key.
     */
    private PartitionValue partitionValue = new PartitionValue();
    
    /**
     * Frame width
     */
    private int frameWidth = 0;
    
    /**
     * Frame height
     */
    private int frameHeight = 0;
    
    /**
     * Pixels to static partition mapping 
     */
    private short[][] staticMapping = null;
    
    /**
     * Pixels to dynamic partition mapping
     */
    private short[][] dynamicMapping = null;
    
    @Override
    public void configure(JobConf conf) {
        super.configure(conf);
        XPCSConfig config = new XPCSConfig(conf);
        frameWidth = config.getFrameWidth();
        
        if (config.getIsKinetics()) {
            frameHeight = config.getSliceHeight();
        } else {
            frameHeight = config.getFrameHeight();
        }
        
        try {
            buildPartitionsMaps(config);
        } catch (IOException e) {
            throw new RuntimeException("Could not read partition file", e);
        }
    }

    private void buildPartitionsMaps(XPCSConfig configuration) throws IOException {
        QMaps partitions = new QMaps(configuration);
        
        this.staticMapping = partitions.getStaticMapping();
        this.dynamicMapping = partitions.getDynamicMapping();
        
        if (this.staticMapping == null || this.dynamicMapping == null) {
            // TODO: Change these to configuration exceptions.
            throw new RuntimeException("Failed to build partitions maps");
        }
    }

    
    @Override
    public void map(G2Key key, G2Value value,
            OutputCollector<PartitionKey, PartitionValue> output,
            Reporter reporter) throws IOException {

        short dPartition = calcDynamicPartition(key);
        
        if (dPartition > 0) {
            partitionKey.setDynamicPartition(dPartition);
            partitionKey.setTau(key.getTau());
            partitionValue.setG2(value.getG2());
            partitionValue.setiFuture(value.getiFuture());
            partitionValue.setiPast(value.getiPast());
            partitionValue.setStaticPartition(calcStaticPartition(key));
            output.collect(partitionKey, partitionValue);
        }
    }

    private short calcDynamicPartition(G2Key key) {
        return this.dynamicMapping[key.getX()][key.getY()];
    }

    private short calcStaticPartition(G2Key key) {
        return this.staticMapping[key.getX()][key.getY()];
    }

}
