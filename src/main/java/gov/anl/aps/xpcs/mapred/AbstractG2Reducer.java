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
import java.util.Iterator;

import gov.anl.aps.xpcs.config.XPCSConfig;
import gov.anl.aps.xpcs.mapred.io.G2Key;
import gov.anl.aps.xpcs.mapred.io.G2Value;
import gov.anl.aps.xpcs.mapred.io.PixelKey;
import gov.anl.aps.xpcs.mapred.io.PixelSumValue;
import gov.anl.aps.xpcs.mapred.io.PixelValue;
import gov.anl.aps.xpcs.mapred.pixelfilter.PixelFilterStride;
import gov.anl.aps.xpcs.mapred.pixelfilter.PixelFilterSum;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public abstract class AbstractG2Reducer extends MapReduceBase implements 
    Reducer<PixelKey, PixelValue, G2Key, G2Value> {

    
    /**
     * Delay per level
     */
    protected int dpl = 4;
    /**
     * Intensities in a given round of computation
     */
    protected double[] intensities;

    /**
     * Total number of frames.
     */
    protected int frameCount = 0;
    
    /**
     * Key to emit results
     */
    protected G2Key g2Key = new G2Key();

    /**
     * Value for G2key
     */
    protected G2Value g2Value = new G2Value();
    
    /**
     * To output sum of individual pixels.
     */
    protected PixelSumValue pixelSum = new PixelSumValue();
    
    /**
     * Multiple output collector for writing pixel sums results separately
     */
    protected MultipleOutputs mos = null;
    
    /**
     * Pixel sums output collector
     */
    protected OutputCollector pixelSumsOutput = null;
    
    /**
     * Static frame window
     */
    protected int frameWindow;
    
    /**
     * Kinetics mode enabled?
     */
    protected boolean isKineticsMode;
    
    /**
     * Kinetics slices per frame
     */
    protected int slicesPerFrame;

    private PixelFilterStride pixelFilterStride = null;

    private PixelFilterSum pixelFilterSum = null;
    
    private XPCSConfig config = null;

    private double[] tempIntensities;

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        this.config = new XPCSConfig(job);
        dpl = config.getDPL();
        frameCount = config.getFramecount();
        frameWindow = config.getStaticWindow();
        slicesPerFrame = config.getLastSlice() - config.getFirstSlice() + 1;
        
        tempIntensities = new double[frameCount];

        if (frameCount < 1) {
            throw new RuntimeException("Failed to read frame count in g2 job");
        }

        mos = new MultipleOutputs(job);
        isKineticsMode = config.getIsKinetics();

        pixelFilterStride = new PixelFilterStride(config);
        pixelFilterSum = new PixelFilterSum(config);

    }
    
    @Override
    public void close() throws IOException {
        this.mos.close();
    }
    
    @Override
    public void reduce(PixelKey key, Iterator<PixelValue> values,
            OutputCollector<G2Key, G2Value> output, Reporter reporter)
            throws IOException {

        Arrays.fill(tempIntensities, 0.0d);
        readIntensities(values);
        multitau(key, output, reporter);
    }
    
    protected void readIntensities(Iterator<PixelValue> it) {

        while (it.hasNext()) {
            PixelValue value = it.next();
            tempIntensities[value.getFrameIndex()] = value.getPixelIntensity();
        }

        intensities = tempIntensities;
        
        if (this.config.getPixelFilterStride() > 1) {
            intensities = pixelFilterStride.apply(intensities);
        }

        if (this.config.getPixelFilterSum() > 1) {
            intensities = pixelFilterSum.apply(intensities);
        }
    }

    protected void emit(double BigG2[], OutputCollector<G2Key, G2Value> output)
            throws IOException {
        g2Value.setG2(BigG2[0]);
        g2Value.setiFuture(BigG2[1]);
        g2Value.setiPast(BigG2[2]);
        output.collect(g2Key, g2Value);
    }
    
    /**
     * When computing G2, we ignore the last odd frame in levels above 1.
     * 
     * @param intensities
     * @param level
     * @return
     */
    protected boolean isOddFrame(int lastframe, int level) {
        int fc = frameCount % 2 == 0 || level == 0 ? frameCount : 
                                                     frameCount - 1;
        
        return (lastframe > fc);
    }
    
    protected void emitPixelSums(double[] intensities, 
                                 PixelKey key, 
                                 Reporter reporter) throws IOException {
        
        if (pixelSumsOutput == null
                && (pixelSumsOutput = mos.getCollector("pixels", reporter)) == null) {
            return;
        }
        
        double pixelsSum = 0;
        double pixelsPartialSum = 0;
        short windowID = 1;
        for (int i=0; i<intensities.length; i++) {
            pixelsSum += intensities[i];
            pixelsPartialSum += intensities[i];
            
            if (frameWindow > 0 && frameWindow != frameCount) {
                if ( i != 0 && (i % frameWindow == 0) ){
                    if (pixelsPartialSum != 0) {
                        pixelSum.setPixelSum(pixelsPartialSum);
                        pixelSum.setWindow(windowID);
                        pixelSumsOutput.collect(key, pixelSum);
                    }
                    pixelsPartialSum = 0;
                    windowID++;
                }
            }
        }
        
        // Complete sum of the pixel intensities
        if (pixelsSum != 0) {
            pixelSum.setPixelSum(pixelsSum);
            pixelSum.setWindow((short) 0);
            pixelSumsOutput.collect(key, pixelSum);
        }
    }
    
    protected abstract void multitau(PixelKey pixelKey,
        OutputCollector<G2Key, G2Value> output, Reporter reporter) 
            throws IOException;
    
}
