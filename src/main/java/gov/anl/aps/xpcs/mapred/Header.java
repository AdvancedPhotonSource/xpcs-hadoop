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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Read the IMM header
 */

public class Header {
    public static final int HEADER_SIZE_IN_BYTES = 1024;
    private int pixelCount;
    private int frameIndex;
    private int widthInPixels;
    private int heightInPixels;
    private int corecoTick;
    private int bytesPerPixel;
    private int immVersion;
    private int number;
    // private int cameraType;
    private double clock;
    
    public Header() {
        // this.cameraType = camerType;
    }
    
    public void update(byte[] buffer) throws IOException {
        ByteBuffer header = ByteBuffer.wrap(buffer, 0, HEADER_SIZE_IN_BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);

        this.setNumber(header.get(14*4));
        this.setClock(header.getDouble(16*8));
        this.setHeightInPixels(header.getInt(27*4));
        this.setWidthInPixels(header.getInt(28*4));
        this.setBytesPerPixel(header.getInt(29*4));
        this.setPixelCount(header.getInt(38*4));
        this.setFrameIndex(header.getInt(40*4));
        this.setImmVersion(header.get(154*4));
        this.setCorecoTick(header.getInt(155*4));
    }

    /**
     * @return the pixelCount
     */
    public int getPixelCount() {
        return pixelCount;
    }

    /**
     * @param clock
     *            the clock to set
     */
    public void setClock(double clock) {
        this.clock = clock;
    }

    /**
     * @return the clock
     */
    public double getClock() {
        return clock;
    }

    /**
     * @param pixelCount
     *            the pixelCount to set
     */
    public void setPixelCount(int pixelCount) {
        this.pixelCount = pixelCount;
    }

    /**
     * @return the frameIndex
     */
    public int getFrameIndex() {
        return frameIndex;
    }

    /**
     * @param frameIndex
     *            the frameIndex to set
     */
    public void setFrameIndex(int frameIndex) {
        this.frameIndex = frameIndex;
    }

    /**
     * @param heightInPixels
     *            the heightInPixels to set
     */
    public void setHeightInPixels(int heightInPixels) {
        this.heightInPixels = heightInPixels;
    }

    /**
     * @param widthInPixels
     *            the widthInPixels to set
     */
    public void setWidthInPixels(int widthInPixels) {
        this.widthInPixels = widthInPixels;
    }

    /**
     * @return the heightInPixels
     */
    public int getHeightInPixels() {
        return heightInPixels;
    }

    /**
     * @return the widthInPixels
     */
    public int getWidthInPixels() {
        return widthInPixels;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append(this.heightInPixels)
                .append(this.widthInPixels).append(this.pixelCount).toString();
    }

    public int getCorecoTick() {
        return corecoTick;
    }

    public void setCorecoTick(int corecoTick) {
        this.corecoTick = corecoTick;
    }

    public int getBytesPerPixel() {
        return bytesPerPixel;
    }

    public void setBytesPerPixel(int bytesPerPixel) {
        this.bytesPerPixel = bytesPerPixel;
    }

    public int getImmVersion() {
        return immVersion;
    }

    public void setImmVersion(int immVersion) {
        this.immVersion = immVersion;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    // public int getCameraType() {
    //     return cameraType;
    // }

    // public void setCameraType(int cameraType) {
    //     this.cameraType = cameraType;
    // }

    // public double getFrameTime() {
        
    //     double frameTime = -99999.0f;
    //     if (cameraType == 8 || cameraType == 13) {
    //         frameTime = getClock();
    //     } else if (cameraType == 5 || cameraType == 6 || cameraType == 15) {
    //         frameTime = getCorecoTick() / 1000000.0d;
    //     }
    //     return frameTime;
    // }
}
