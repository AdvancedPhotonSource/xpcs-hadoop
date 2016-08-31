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

package gov.anl.aps.xpcs.mapred.pixelfilter;

import gov.anl.aps.xpcs.config.XPCSConfig;

public class PixelFilterSum implements PixelFilter {

	private int offset = 1;

    private int frameCount = 0;

    private double[] intensitiesOut = null;

	public PixelFilterSum(XPCSConfig config) {
		frameCount = config.getFramecount();
        offset = config.getPixelFilterSum();
	}

	// Bin the original image.
	public double[] apply(double intensities[]) {

        if (intensitiesOut == null)
        {
            intensitiesOut = new double[intensities.length / offset];
        }

        int k = 0;

        for (int i = 0 ; i < intensities.length; i+=offset)
        {
            if (k >= intensitiesOut.length) break;
            
            intensitiesOut[k] = 0.0d;

            for (int j = 0; j < offset; j++)
            {
                intensitiesOut[k] += intensities[i + j];
            }

            intensitiesOut[k] /= offset;
            
            k++;
        }

        return intensitiesOut;
	}  
}