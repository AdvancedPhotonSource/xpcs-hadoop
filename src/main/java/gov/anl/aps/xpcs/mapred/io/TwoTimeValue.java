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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TwoTimeValue implements Writable {

    private static final String SEP = ",";
    protected int frameI;
    protected int frameJ;
    protected double twoTime;

    public TwoTimeValue() {
        super();
    }

    public TwoTimeValue(double value, int frameI, int frameJ) {
        super();
        this.twoTime = value;
        this.frameI = frameI;
        this.frameJ = frameJ;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.frameI = input.readInt();
        this.frameJ = input.readInt();
        this.twoTime = input.readDouble();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(this.frameI);
        output.writeInt(this.frameJ);
        output.writeDouble(this.twoTime);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.frameI);
        sb.append(SEP);
        sb.append(this.frameJ);
        sb.append(SEP);
        sb.append(this.twoTime);
        
        return sb.toString();
    }

    public double getTowTime() {
        return this.twoTime;
    }

    public void setTwoTime(double twoT) {
        this.twoTime = twoT;
    }

    public int getFrameI() {
        return this.frameI;
    }

    public void setFrameI(int fi) {
        this.frameI = fi;
    }

    public int getFrameJ() {
        return this.frameJ;
    }

    public void setFrameJ(int fj) {
        this.frameJ = fj;
    }
}
