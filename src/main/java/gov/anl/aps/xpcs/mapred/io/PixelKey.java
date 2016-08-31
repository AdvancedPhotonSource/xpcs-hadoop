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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PixelKey implements WritableComparable<PixelKey> {
    private static final String SEP = ",";
    private short X;
    private short Y;
    private StringBuilder sb = new StringBuilder(30);
    
    public PixelKey() {
        super();
    }
    
    public PixelKey(short x, short y) {
        super();
        this.X = x;
        this.Y = y;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        this.X = input.readShort();
        this.Y = input.readShort();
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeShort(this.X);
        output.writeShort(this.Y);
    }

    public short getX() {
        return this.X;
    }

    public void setX(short x) {
        this.X = x;
    }
    
    public short getY() {
        return this.Y;
    }
    
    public void setY(short y) {
        this.Y = y;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PixelKey) {
            PixelKey key = (PixelKey) obj;
            return this.X == key.X && this.Y == key.Y;
        }
        return false;
    }
    
    @Override
    public String toString() {
        // reuse the sb
        sb.setLength(0);

        sb.append(this.X);
        sb.append(SEP);
        sb.append(this.Y);
        
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.X)
                                    .append(this.Y)
                                    .toHashCode();
    }
    
    @Override
    public int compareTo(PixelKey that) {
        if (this.X != that.X) {
            return this.X < that.X ? -1 : 1;
        }
        else if (this.Y != that.Y){
            return this.Y < that.Y ? -1 : 1;
        }
                
        return 0;
    }
    
    // TODO : Check if these comparator in multiple classes can be generalized.
    public static class PixelKeyComparator extends WritableComparator {
        public PixelKeyComparator() {
            super(PixelKey.class);
        }
        
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }
    
    static {
        WritableComparator.define(PixelKey.class, 
                                  new PixelKeyComparator());
    }

    

}
