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

import gov.anl.aps.xpcs.mapred.io.G2Key;
import gov.anl.aps.xpcs.mapred.io.G2Value;
import gov.anl.aps.xpcs.mapred.io.PixelKey;
import gov.anl.aps.xpcs.multitau.G2;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class G2KineticsReducer extends AbstractG2Reducer {
    
    @Override
    public void configure(JobConf job) {
        super.configure(job);
        intensities = new double[frameCount * slicesPerFrame];
    }

    protected void multitau(PixelKey pixelKey, 
        OutputCollector<G2Key, G2Value> output, Reporter reporter)
            throws IOException {
        
        g2Key.setX(pixelKey.getX());
        g2Key.setY(pixelKey.getY());
        
        double[] G2result = new double[3];
        
        int tau = 1;
        for (; tau <= slicesPerFrame - 1; tau++) {
            g2Key.setTau(-1 * tau);
            // Run G2 on all slices (total slices are intensities.length)
            // Level is always 0 when computing slices co-relation in kinetics mode. 
            G2.computeG2Kinetics(intensities, G2result, intensities.length, tau, slicesPerFrame);
            emit(G2result, output);
        }
        
        // Compute co-relation for single overlapping frames across the kinetics data.
        g2Key.setTau(-1 * tau); // Use the tau from the last increment in the loop above. 
        G2.computeG2KineticsOverlap(intensities, G2result, intensities.length, slicesPerFrame);
        emit(G2result, output);
        
        // Kinetics mode pixel sum is based on per slice. 
        emitPixelSums(intensities, pixelKey, reporter);
        
        // Average out slices to form a single frame out of slicesPerFrame
        // The G2 for frame is now going to be on total number of frames averaged out. 
        int frames = G2.avergeOutSlices(intensities, slicesPerFrame);
        int maxLevel = G2.calculateLevelMax(frames, dpl);
        tau = 1;
        int level = 0;
        while (level <= maxLevel) {
            int tauIncrement = (int) Math.pow(2, level);
            G2.smooth(intensities, frames, tauIncrement);
            int dplCount = G2.calculateDelayCount(dpl, level);
            for (int delayIndex = 0; delayIndex < dplCount; delayIndex++) {
                
                if (isOddFrame(tau+tauIncrement, level)) {
                    break;
                }
                
                g2Key.setTau(tau);
                G2.computeG2(intensities, G2result, frames, tau, level);
                emit(G2result, output);
                tau += tauIncrement;
            }
            level++;
        }
    }

}
