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

package gov.anl.aps.xpcs.mapred.io;

import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.nio.BufferUnderflowException;

import java.util.HashSet;

public class Frame {
    
    /**
     * Width of XPCS image frame
     */
    private int frameWidth;
    
    /**
     * Height of XPCS image frame
     */
    private int frameHeight = 0;
    
	 /**
     * Total number of pixels in current frame.
     */
    private int pixelCounts = 0;
    
    /**
     * Index of current frame
     */
    private int frameIndex;

    private int relativeFrameIndex;

    /**
     * Kinetics mode pixel index offset.
     * In kinetics mode the pixel indices have two value within slice and within frame.
     * The within slice index is usually an offset from the beginning of the frame.
     */
    private int kineticsPixelOffset = 0;
        
    /**
     * Buffer for reading pixel values. 
     */
    private ShortBuffer valueBuffer = null;
    
    /**
     * Buffer for reading pixel indices. 
     */
    private IntBuffer indexBuffer = null;
    
    private double frameSum = 0;

   	private int[]  pixelIndices = null;

    private float[]  pixelValues = null;

    private double frameClock = 0.0d;

    private double frameTick = 0.0d;

    private int frameOffset = 0;

	// Frame constructor. The frame object can be reused
    // as long as the height and width remains the same.
    // 
    public Frame(int frameWidth, int frameHeight, int frameOffset) {
    	this.frameWidth = frameWidth;
    	this.frameHeight = frameHeight;
        this.frameOffset = frameOffset;
        this.frameSum = 0.0;
    }
    
	public void setImage(int[] index, 
                        float[] values, 
                        int pixels, 
                        int frameNo,
                        double clock,
                        double tick) {
    	this.pixelIndices = index;
    	this.pixelValues = values;
    	this.pixelCounts = pixels;
    	this.frameIndex = frameNo;
    	this.frameClock = clock;
        this.frameTick = tick;
    }
   
    public float[] getPixels() {
    	return this.pixelValues;
    }
    
    public void setPixels(float f[]) {
    	this.pixelValues = f;
    }
    
    public int[] getIndices() {
        return this.pixelIndices;
    }
    
    public void setPixelIndices(int[] i) {
    	this.pixelIndices = i;
    }

    public IntBuffer getIndexBuffer() {
    	return this.indexBuffer;
    }
    
    public ShortBuffer getValueBuffer() {
        return this.valueBuffer;
    }

	public int getPixelCounts() {
		return pixelCounts;
	}
	
	public void setPixelCounts(int count) {
		this.pixelCounts = count;
	}
	
	public double getFrameSum() {
		return frameSum;
	}

	public void setFrameSum(double frameSum) {
		this.frameSum = frameSum;
	}

	public int getFrameIndex() {
		return frameIndex - this.frameOffset;
	}

    public int getFrameActualIndex() {
        return frameIndex;
    }

    public int getWidth() {
        return this.frameWidth;
    }
    
    public int getHeight() {
        return this.frameHeight;
    }
    
    public double getFrameClock() {
		return frameClock;
	}

	public void setFrameClock(double clock) {
		this.frameClock = clock;
	}

    public double getFrameTick() {
        return frameTick;
    }

    public void setFrameTick(double tick) {
        this.frameTick = tick;
    }

    public void setKineticsPixelOffset(int offset) {
        this.kineticsPixelOffset = offset;
    }

    public int getKineticsPixelOffset() {
        return this.kineticsPixelOffset;
    }


    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Frame - ");
        sb.append(this.frameIndex);
        sb.append(" [ width = '");
        sb.append(this.frameWidth);
        sb.append(" ] ");
        sb.append(" [ height = '");
        sb.append(this.frameHeight);
        sb.append(" ] ");
        sb.append(" [ pixels = ");
        sb.append(this.pixelCounts);
        sb.append(" ] ");

        if (pixelIndices != null && pixelIndices.length > 0)
            sb.append(" Value at pixel " + this.pixelIndices[0]);

        if (pixelValues != null && pixelValues.length > 0)
            sb.append(" is " + this.pixelValues[0]);

        return sb.toString();
    }
}
