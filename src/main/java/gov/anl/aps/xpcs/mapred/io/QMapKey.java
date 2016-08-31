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

public class QMapKey implements WritableComparable<QMapKey> {

    private static final String SEP = ",";
    private short q;
    private int pixelIndex;
   
    public QMapKey() {
        super();
    }
    
    public QMapKey(short q, int pIndex) {
        super();
        this.q = q;
        this.pixelIndex = pIndex;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(this.q);
        out.writeInt(this.pixelIndex);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.q = in.readShort();
        this.pixelIndex = in.readInt();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.q)
                                    .toHashCode();
    }
    
 	public short getQ() {
		return q;
	}

	public void setQ(short q) {
		this.q = q;
	}
	
	public int getPixelIndex() {
		return this.pixelIndex;
	}
	
	public void setPixelIndex(int ind) {
		this.pixelIndex = ind;
	}
	
	@Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.q);
        builder.append(SEP);
        builder.append(this.pixelIndex);
        
        return builder.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QMapKey) {
            QMapKey key = (QMapKey) obj;
            return new EqualsBuilder().append(this.q, key.getQ())
                    				  .isEquals();
        }
        return false;
    }

    @Override
    public int compareTo(QMapKey that) {
    	int res = 0;
    	
        if (this.q != that.q) {
        	res = this.q < that.q ? -1 : 1;
        } else if (this.pixelIndex != that.pixelIndex){
        	res = this.pixelIndex < that.pixelIndex ? -1 : 1;
        }
        
        return res;
    }
    
    public int compareNaturalKey(QMapKey that) {
    	if (this.q != that.q) {
    		return this.q < that.q ? -1 : 1;
    	}
    	return 0;
    }
    
//    // TODO : Check if these comparator in multiple classes can be generalized.
//    public static class G2KeyComparator extends WritableComparator {
//        public G2KeyComparator() {
//            super(QMapKey.class);
//        }
//        
//        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//            return compareBytes(b1, s1, l1, b2, s2, l2);
//        }
//    }
//    
//    static {
//        WritableComparator.define(QMapKey.class, 
//                                  new G2KeyComparator());
//    }
}
