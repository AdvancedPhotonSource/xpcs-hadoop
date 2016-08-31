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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

import gov.anl.aps.xpcs.config.XPCSConfig;
import gov.anl.aps.xpcs.mapred.io.G2Key;
import gov.anl.aps.xpcs.mapred.io.G2Value;
import gov.anl.aps.xpcs.mapred.io.PixelKey;
import gov.anl.aps.xpcs.mapred.io.PixelSumValue;
import gov.anl.aps.xpcs.mapred.io.PixelValue;
import gov.anl.aps.xpcs.mapred.io.QMapKey;
import gov.anl.aps.xpcs.mapred.io.QMapValue;
import gov.anl.aps.xpcs.mapred.io.TwoTimeBinaryValue;
import gov.anl.aps.xpcs.mapred.io.PixelSumValue;
import gov.anl.aps.xpcs.multitau.G2;
import gov.anl.aps.xpcs.util.QMaps;
import gov.anl.aps.xpcs.util.FrameSum;
import gov.anl.aps.xpcs.util.PixelSum;
import gov.anl.aps.xpcs.util.SmoothedSG;
import gov.anl.aps.xpcs.mapred.pixelfilter.PixelFilterStride;
import gov.anl.aps.xpcs.mapred.pixelfilter.PixelFilterSum;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TwoTimesReducer extends MapReduceBase implements 
Reducer<QMapKey, QMapValue, QMapKey, TwoTimeBinaryValue> {
    
    private static final Logger logger = LoggerFactory.getLogger(TwoTimesReducer.class
            .getName());


	private double[] intensities;
	
	private byte[] twotimes;

    private double[] twotimesBuffer = null;

	private int frameCount;
	
	private QMapValue lastvalue = null;

        /**
     * To output sum of individual pixels.
     */
    // protected PixelSumValue pixelSum = new PixelSumValue();
	
	private TwoTimeBinaryValue twotime = new TwoTimeBinaryValue();

    private int frameWidthBin = 0;
    
    private int frameHeightBin = 0;

    private double frameSum[] = null;

    private double smoothedSG[] = null;

    private int lastQValue = -1;

    // private int[] pixelsPerBin = null;

    private PixelFilterStride pixelFilterStride = null;
    private PixelFilterSum pixelFilterSum = null;
    
    private XPCSConfig config = null;

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        config = new XPCSConfig(job);

        frameCount = config.getFramecount();
        intensities = new double[frameCount];

        //twotimes = new byte[(frameCount * frameCount * 4) + 8];
        twotimesBuffer = new double[frameCount * frameCount]; //ByteBuffer.wrap(twotimes).asDoubleBuffer();

        this.frameWidthBin = config.getFrameWidth() / (int) config.getBinX();

        if (frameCount < 1) {
            throw new RuntimeException("Failed to read frame count in g2 job");
        }

        try {
            FrameSum fsum = new FrameSum(config);
            frameSum = fsum.getFrameSum();

            SmoothedSG smg = new SmoothedSG(config);
            smoothedSG = smg.getSmoothedSG();    

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
  
    @Override
    public void reduce(QMapKey key, Iterator<QMapValue> values,
            OutputCollector<QMapKey, TwoTimeBinaryValue> output, Reporter reporter)
            throws IOException {

        logger.info("Processing qmap " + key.getQ());
        
        // Go through the values within a same pixel group to compute
    	// correlation. 
        while (values.hasNext()) {

            if (lastQValue == -1) {
                lastQValue = key.getQ();
            }
            else {
                if (lastQValue != key.getQ())
                    throw new IOException("A TwoTime reducer is intended only to process a single q-bin. Exiting...");
            }

        	Arrays.fill(intensities, 0.0d);
            readIntensities(values);   
            update2T();
        }

        this.twotimesBuffer[0] = key.getQ();
        this.twotime.setData(this.twotimesBuffer);
        output.collect(key, this.twotime);
    }

    private void update2T() {
        int index = 1;

    	for (int i = 0 ; i < this.intensities.length; i++) {

            if (intensities[i] == 0) {
                index += ( (frameCount - 1) - i);
                continue;
            }

    		for (int j = i + 1 ; j < this.intensities.length; j++) {

                // if (intensities[j] == 0) {
                //     index++;
                //     continue;
                // }

                // Multiply intensities for a single pixel. The addition sums  up the
                // intensity product for all pixels for frame # i and frame # j. 

                // index = i * (this.frameCount - i) + j; // Only saving upper traiagnular matrix

                twotimesBuffer[index] += this.intensities[i] * this.intensities[j]; 
                index++;
    		}
    	}
    }

    protected void readIntensities(Iterator<QMapValue> it) {
        int pixelmark = -1;
        if (lastvalue != null) {
            
            if (lastvalue.getFrameValue() != 0 && 
                    smoothedSG[lastvalue.getIndex()] != 0 && 
                    frameSum[lastvalue.getFrameIndex()] != 0)

                intensities[lastvalue.getFrameIndex()] = lastvalue.getFrameValue() / 
                                                smoothedSG[lastvalue.getIndex()] / 
                                                    frameSum[lastvalue.getFrameIndex()];
      
    		pixelmark = lastvalue.getIndex();
            lastvalue = null;
    	}
    	    	
        while (it.hasNext()) {
        	QMapValue value = it.next();
     
        	// If pixel mark is not already set, we can set it here to current value's pixel index.
        	if (pixelmark == -1) {
        		pixelmark = value.getIndex();
        	} 
        	
        	if (pixelmark != value.getIndex()) {
        		lastvalue = value;
        		break;
        	}

            if (value.getFrameValue() != 0 && 
                    smoothedSG[value.getIndex()] != 0 && 
                    frameSum[value.getFrameIndex()] != 0) {
                
               intensities[value.getFrameIndex()] = value.getFrameValue() / smoothedSG[value.getIndex()] / frameSum[value.getFrameIndex()]; 
            }

        }

        if (this.config.getPixelFilterStride() > 1)
        {
            intensities = pixelFilterStride.apply(intensities);
        }

        if (this.config.getPixelFilterSum() > 1)
        {
            intensities = pixelFilterSum.apply(intensities);
        }
        
    }
}
