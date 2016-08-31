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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

public class KineticsImageFormat implements ImageFormat {

    protected static final int BYTES_PER_PIXEL_VALUE = 2;
    
    protected static final int BYTES_PER_PIXEL_INDEX = 4;
    
    protected static final int BYTES_PER_PIXEL = BYTES_PER_PIXEL_VALUE
            + BYTES_PER_PIXEL_INDEX;

    private Header currentHeader = null;
    
    private byte[] buffer = null;
    
   	private int[]  pixelIndices = null;

    private float[]  pixelValues = null;
    
    /**
     * Kinetics mode beginning slice to process.  
     */
    private int firstSlice = 0;
    
    /**
     * Kinetics mode last slie to process
     */
    private int lastSlice = 0;
    
    /**
     * Kinetics mode slice height in #of rows.
     */
    private int sliceHeight = 0;
    
    /**
     * Kinetics mode slice starting row #.
     */
    private int sliceTop = 0;
    
    /**
     * Index of slice being processed.  
     */
    private int kineticsSlicesIndex = 0;
    
    /**
     * Total number of slices per frame.
     */
    private int kineticsSlicesPerFrame = 0;
    
    /**
     * Index of the starting pixel in the frame. 
     */
    private int kineticsFirstPixel = 0;
    
    private int kineticsFrameIndex = 0;
    
    private int kineticsPixelIndex = 0;
    
    private int kineticsPixelCount = 0;
    
    private int frameWidth = 0;
    
    private int frameHeight = 0;
    
    private int frameStart = 0;

    private int sliceNumber = 0;
    
    private ShortBuffer valueBuffer = null;

    public KineticsImageFormat(XPCSConfig config) {
        currentHeader = new Header();
        this.frameWidth = config.getFrameWidth();
        this.frameHeight = config.getFrameHeight();
        this.frameStart = config.getFirstFrame();
        this.firstSlice = config.getFirstSlice();
        this.lastSlice = config.getLastSlice();
        this.sliceHeight = config.getSliceHeight();
        this.sliceTop = config.getSliceTop();
        
        kineticsSlicesPerFrame = (this.lastSlice - this.firstSlice) + 1;
        kineticsPixelCount = this.sliceHeight * this.frameWidth;
        int totalKineticsSlices = this.frameHeight / this.sliceHeight;
        // From the index of last slice (row), we subtract by total number
        // of pixels between the actual last slice and first usable slice. 
        // The usable slice is specified by the user. 
        this.kineticsFirstPixel = (this.sliceTop - 
            ((totalKineticsSlices - this.firstSlice + 1) * this.sliceHeight)) * this.frameWidth; 

        buffer = new byte[BYTES_PER_PIXEL_VALUE * frameWidth * frameHeight];
        
		pixelIndices = new int[kineticsPixelCount];
		pixelValues = new float[kineticsPixelCount];
    }

    @Override
    public void readNextFrame(FSDataInputStream fileIn, Frame f) throws IOException {
		if (valueBuffer == null
				|| this.kineticsSlicesIndex == 0) {
    		// read next frame
    		fileIn.readFully(buffer, 0, Header.HEADER_SIZE_IN_BYTES);
            currentHeader.update(buffer);
            int frameIndex = currentHeader.getFrameIndex();
            int pixelCounts = currentHeader.getPixelCount();
     
            fileIn.readFully(buffer, 0, BYTES_PER_PIXEL_VALUE * pixelCounts);
            valueBuffer = ByteBuffer
                    .wrap(buffer, 0, BYTES_PER_PIXEL_VALUE * pixelCounts)
                    .order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();

            // Setup the kinetics pixel index, reset only when we read the next full frame.
            // Once the index is set, it linearly increments as all slices are contiguous. 
            //  A -1 as next method will increment by 1 before looking up the index.
            this.kineticsPixelIndex = this.kineticsFirstPixel;

            // This gives the last effective slice in this frame.
            this.kineticsFrameIndex = 
            		(frameIndex - this.frameStart + 1) * this.kineticsSlicesPerFrame - 1;

            // Subtract total slices per frame to get the starting slice number in this frame. 
            this.kineticsFrameIndex -= (this.kineticsSlicesPerFrame - 1);
            this.kineticsSlicesIndex = this.kineticsSlicesPerFrame - 1;

            // The current slice number within this frame that we are going to process. 
            this.sliceNumber = 2;
    	} else {
            this.kineticsSlicesIndex--;
            this.kineticsFrameIndex++;
            this.sliceNumber++;
    	}
		
		int cnt = 0;
		short value = 0;

        this.kineticsPixelIndex = (this.sliceTop - (this.sliceNumber * this.sliceHeight)) * this.frameWidth;

        f.setKineticsPixelOffset(this.kineticsPixelIndex);
        
		while (cnt < this.kineticsPixelCount) {
			value = valueBuffer.get(this.kineticsPixelIndex++);
			pixelIndices[cnt] = cnt;
			pixelValues[cnt++] = value;
		}

		f.setImage(pixelIndices, 
                   pixelValues, 
                   this.kineticsPixelCount,
				   this.kineticsFrameIndex, 
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
                    (pixelCounts * BYTES_PER_PIXEL_VALUE));
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

    /**
     * Tells whether the current 'physical' frame has more slices to be processed or not. 
     */
    public boolean hasSlicesInFrame()
    {
        return this.kineticsSlicesIndex != 0;
    }

}
