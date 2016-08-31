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

package gov.anl.aps.xpcs.mapred.filter;

import gov.anl.aps.xpcs.mapred.io.Frame;
import gov.anl.aps.xpcs.config.XPCSConfig;

import org.apache.hadoop.conf.Configuration;

import java.util.Random;

public class BinningFilter implements Filter {
    
    private int xbin = 1;
    private int ybin = 1;

    public BinningFilter(XPCSConfig configuration) {
    	xbin = (int) configuration.getBinX();
    	ybin = (int) configuration.getBinY();
    }

    public void apply(Frame f) {
        int width = f.getWidth();
        int height = f.getHeight();
        
        Binning.bin(f.getPixels(), width, height, xbin, ybin);
        
		int xbins = (int) Math.floor((double) width / xbin);
		int ybins = (int) Math.floor((double) height / ybin);

        f.setPixelCounts(xbins * ybins);
    }

    public static void main(String args[]) {
        XPCSConfig config = new XPCSConfig(new Configuration());
        config.setInputFilePath("/Users/faisal/Projects/MultiTau/data_feb_2015/AA_test_covalent_PEG9k_080C_Fq1_001_0001-3072_bin5Y.hdf");
        config.setBinX(1.0f);
        config.setBinY(5.0f);
        BinningFilter filter = new BinningFilter(config);

        int width  = 1024;
        int height = 1024;

        Random generator = new Random();

        float f[] = new float[width * height];

        for (int i = 0 ; i < (width*height); i++) {
            f[i] = generator.nextInt(10);
        }
        
        Frame frame = new Frame(width, height, 0);
        frame.setPixels(f);
        filter.apply(frame);
    }

}
