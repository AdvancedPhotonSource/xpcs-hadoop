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

package gov.anl.aps.xpcs.multitau;

public class G2 {
    
    /**
     * Max level that can be reached for given frame-count and delay per levels.
     * @param frameCount
     * @param dpl
     * @return
     */
    public static int calculateLevelMax(int frameCount, int dpl) {
        if (frameCount < dpl * 2) return 0;
        
        return (int) (Math.floor(log2(frameCount) - 
                log2(1d + 1d/(double) dpl)) - log2(dpl));
    }
    
    /**
     * For given set of intensities, tau value and tau-step (level), compute
     * the g2 function.
     * 
     * @param intensities
     * @param tau
     * @param level
     * @return
     */
    public static double[] computeG2(double[] intensities,
                                     double[] result,
                                     int frameCount, 
                                     int tau, 
                                     int level) {
        int tauIncrement = level == 0 ? 1 : (int) Math.pow(2, level);
        double numerator = 0.0d;
        double sumPast = 0.0d;
        double sumFuture = 0.0d;
        int count = 0;

        int requiredFrames = tau + tauIncrement - 1;
        int remainingFrames = frameCount - 1;
        
        int frameIndex = 0;
        while (remainingFrames >= requiredFrames) {

            numerator += intensities[frameIndex] * intensities[frameIndex + tau];
            sumPast += intensities[frameIndex];
            sumFuture += intensities[frameIndex + tau];
            count++;
            
            remainingFrames -= tauIncrement;
            frameIndex += tauIncrement;
        }
        
        result[0] = (double) numerator / (double) count;
        result[1] = (double) sumFuture / (double) count;
        result[2] = (double) sumPast / (double) count;
        
        return result;
    }
    
    /**
     * Compute G2 values for Kinetics mode frame slices. 
     * 
     * @param intensities Intensity values for a given pixel across frames.  
     * @param result Array of size 3, where to put the result of computation.
     * @param frameCount Total number of frames to process in intensity array,
     *                      should be less than intensities.length
     * @param tau   The tau parameter for the step function. 
     * @param slicesPerFrame Total number of slices in each frame. 
     */
    public static void computeG2Kinetics(double[] intensities,
                                         double[] result,
                                         int frameCount, 
                                         int tau,
                                         int slicesPerFrame) {

        int tauIncrement = 1;
        double numerator = 0.0d;
        double sumPast = 0.0d;
        double sumFuture = 0.0d;
        int count = 0;

        int requiredFrames = tau + tauIncrement - 1;
        int remainingFrames = frameCount - 1;
        
        int frameIndex = 0;
        while (remainingFrames >= requiredFrames) {

            if (!overlapsFrameBoundary(frameIndex, tau, slicesPerFrame)) {
                numerator += intensities[frameIndex] * intensities[frameIndex + tau];
                sumPast += intensities[frameIndex];
                sumFuture += intensities[frameIndex + tau];
                count++;
            }
            
            remainingFrames -= tauIncrement;
            frameIndex += tauIncrement;
        }
        
        result[0] = (double) numerator / (double) count;
        result[1] = (double) sumFuture / (double) count;
        result[2] = (double) sumPast / (double) count;
        
    }
    
    
    /**
     * Compute {@link G2} values for usable slices at the edge of frameN and frameN+1
     * @param intensities
     * @param result
     * @param frameCount
     * @param tau
     * @param level
     * @param slicesPerFrame
     */
    public static void computeG2KineticsOverlap(double[] intensities,
                                         double[] result,
                                         int frameCount, 
                                         int slicesPerFrame) {
        double numerator = 0.0d;
        double sumPast = 0.0d;
        double sumFuture = 0.0d;
        int count = 0;

        // Start at the last slice of first frame. 
        int sliceN = 0;  //slicesPerFrame - 1;
        int sliceN1 = (2 * slicesPerFrame) - 1;
        while ( sliceN1 <= (frameCount - 1) ) {
            numerator += intensities[sliceN] * intensities[sliceN1];
            sumPast += intensities[sliceN];
            sumFuture += intensities[sliceN1];
            count++;
            
            sliceN += slicesPerFrame;
            sliceN1 += slicesPerFrame;
        }
        
        result[0] = (double) numerator / (double) count;
        result[1] = (double) sumFuture / (double) count;
        result[2] = (double) sumPast / (double) count;
    }
    
    /**
     * For the kinetic case ensure that the points at frameIndex and frameIndex
     * + tau are in the same frame
     * 
     * @param frameIndex
     * @return
     */
    private static boolean overlapsFrameBoundary(int frameIndex, int tau, int slicesPerFrame) {
//       return (frameIndex + tau) % (slicesPerFrame-1) == 0;
        return ((frameIndex + tau) / slicesPerFrame) != ((frameIndex) / slicesPerFrame);
    }

    
      
    public static int calculateDelayCount(int dpl, int level) {
        return level == 0 ? (2 * dpl -1) : dpl; 
    }
    
    /**
     * Apply average smoothing to frame intensities. Only process
     * upto 'icount' number of frames within the given frames. 
     * 
     * @param intensities
     * @param icount
     * @param deltaT
     */
    public static void smooth(double[] intensities, int icount, int deltaT) {
        if (deltaT <= 1) return;
        
        for (int t = 0; t < intensities.length - deltaT / 2; t += deltaT) {
            intensities[t] = (intensities[t] + intensities[t + (deltaT / 2)]) / 2;
        }
    }
    
    /**
     * Average out slices within each frames.
     * 
     *  This function is used to average out kinetics mode slices in each frame.
     *  The 'intensities' array is overwritten to have the average frame intensities
     *  in first 'N' set of indices where N is total number of 'physical' frames.
     * 
     * @param intensities The array of intensities in form of slicesPerFrame * #of frames
     * @param slicesPerFrame
     */
    public static int avergeOutSlices(double[] intensities, int slicesPerFrame)
    {
        
        int j = 0;
        double sum = 0;
        int frames = intensities.length / slicesPerFrame;
        
        for (int i = 0; i < intensities.length; i++) {
            sum += intensities[i];
            
            if ( ( (i+1) % slicesPerFrame) == 0 ) {
                intensities[j] = sum / (double) slicesPerFrame;
                sum = 0;
                j++;
            }
        }
        
        return frames;
    }

    /**
     * Compute log base 2 of given value.
     * 
     * @param value
     * @return
     */
    private static double log2(double value) {
        return Math.log(value) / Math.log(2);
    }

}
