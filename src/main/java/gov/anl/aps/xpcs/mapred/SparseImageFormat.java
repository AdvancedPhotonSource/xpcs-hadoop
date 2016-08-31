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
import gov.anl.aps.xpcs.mapred.io.Frame;
import gov.anl.aps.xpcs.mapred.io.PixelKey;
import gov.anl.aps.xpcs.mapred.io.PixelValue;
import gov.anl.aps.xpcs.util.DarkImage;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

public class SparseImageFormat implements ImageFormat {

    protected static final int BYTES_PER_PIXEL_VALUE = 2;
    
    protected static final int BYTES_PER_PIXEL_INDEX = 4;
    
    protected static final int BYTES_PER_PIXEL = BYTES_PER_PIXEL_VALUE
            + BYTES_PER_PIXEL_INDEX;

    private Header currentHeader = null;
    
    private byte[] buffer = null;
    
   	private int[]  pixelIndices = null;

    private float[]  pixelValues = null;
    
    private int fullFrame = 0;

    public SparseImageFormat(XPCSConfig config) {
        currentHeader = new Header();
        fullFrame = config.getFrameWidth() * config.getFrameHeight();
		buffer = new byte[BYTES_PER_PIXEL * fullFrame];
		pixelIndices = new int[fullFrame];
		pixelValues = new float[fullFrame];
    }

    @Override
    public void readNextFrame(FSDataInputStream fileIn, Frame f) throws IOException {
        fileIn.readFully(buffer, 0, Header.HEADER_SIZE_IN_BYTES);
        currentHeader.update(buffer);
        int frameIndex = currentHeader.getFrameIndex();
        int pixelCounts = currentHeader.getPixelCount();

        f.setFrameClock(currentHeader.getClock());
        f.setFrameTick(currentHeader.getCorecoTick());
        
        fileIn.readFully(buffer, 0, BYTES_PER_PIXEL * pixelCounts);

        IntBuffer indexBuffer = ByteBuffer
            .wrap(buffer, 0, BYTES_PER_PIXEL_INDEX * pixelCounts)
                .order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        ShortBuffer valueBuffer = ByteBuffer
            .wrap(buffer, BYTES_PER_PIXEL_INDEX * pixelCounts,
                BYTES_PER_PIXEL_VALUE * pixelCounts)
                    .order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();

        Arrays.fill(pixelValues, 0.0f);
        
        int index = 0;
        short value = 0;
        
		try {
			while (true) {
				index = indexBuffer.get();
				value = valueBuffer.get();
				pixelValues[index] = value;
			}
		} catch (BufferUnderflowException bn) {
		}
        // Update the image contained in the frame. 
        f.setImage(pixelIndices, 
                   pixelValues, 
                   fullFrame, 
                   frameIndex, 
                   currentHeader.getClock(), 
                   currentHeader.getCorecoTick());
    }
   
	public boolean skip(FSDataInputStream fileIn, int toFrame, long lastByte)
			throws IOException {
        boolean found = false;

        while (true) {
             // If we reached the end of the file split
            if (fileIn.getPos() >= lastByte) {
                break;
            }

            fileIn.readFully(buffer, 0, Header.HEADER_SIZE_IN_BYTES);
            currentHeader.update(buffer);
            int frameIndex = currentHeader.getFrameIndex();
            int pixelCounts = currentHeader.getPixelCount();

            if (frameIndex < toFrame) {
                fileIn.seek(fileIn.getPos() + 
                    (pixelCounts * BYTES_PER_PIXEL));
            } else {
            	found = true;
                //TODO: We are seeking back to the beginning of header, this
                // seek can be avoid by keeping the header around for next read
                // request.
                fileIn.seek(fileIn.getPos() - Header.HEADER_SIZE_IN_BYTES);
                break;
            }    
        }

        return found;
    }

}
