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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class G2Key implements WritableComparable<G2Key> {

    private static final String SEP = ",";
    private short X;
    private short Y;
    private int tau;
    
    
    public G2Key() {
        super();
    }
    
    public G2Key(short x, short y, int tau) {
        super();
        this.X = x;
        this.Y = y;
        this.tau = tau;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(this.X);
        out.writeShort(this.Y);
        out.writeInt(this.tau);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.X = in.readShort();
        this.Y = in.readShort();
        this.tau = in.readInt();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(X)
                                    .append(Y)
                                    .append(tau)
                                    .toHashCode();
        
    }
    
    
    public short getX() {
        return X;
    }

    public void setX(short x) {
        X = x;
    }

    public short getY() {
        return Y;
    }

    public void setY(short y) {
        Y = y;
    }

    public int getTau() {
        return tau;
    }

    public void setTau(int tau) {
        this.tau = tau;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.X);
        builder.append(SEP);
        builder.append(this.Y);
        builder.append(SEP);
        builder.append(this.tau);
        
        return builder.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof G2Key) {
            G2Key key = (G2Key) obj;
            return new EqualsBuilder().append(this.X, key.getX())
                    .append(this.Y, key.getY())
                    .append(this.tau, key.getTau()).isEquals();
        }
        return false;
    }

    @Override
    public int compareTo(G2Key that) {
        if (this.X != that.X) {
            return this.X < that.X ? -1 : 1;
        } 
        else if (this.Y != that.Y) {
            return this.Y < that.Y ? -1 : 1;
        } 
        else if (this.tau != that.tau) {
            return this.tau < that.tau ? -1 : 1;
        }
        
        return 0;
    }
    
    // TODO : Check if these comparator in multiple classes can be generalized.
    public static class G2KeyComparator extends WritableComparator {
        public G2KeyComparator() {
            super(G2Key.class);
        }
        
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }
    
    static {
        WritableComparator.define(G2Key.class, 
                                  new G2KeyComparator());
    }

    
}
