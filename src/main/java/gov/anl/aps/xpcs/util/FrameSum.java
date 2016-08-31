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

package gov.anl.aps.xpcs.util;

import gov.anl.aps.xpcs.config.XPCSConfig;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FrameSum {
    
    private static final Logger logger = LoggerFactory.getLogger(FrameSum.class
            .getName());
     /**
     * Wrap buffer data for dynamic partition map
     */
    private ByteBuffer frameSum = null;

    private byte rawFrameSum[] = null;

    private int totalFrames = 0;
 
	public FrameSum(XPCSConfig configuration) throws IOException {
        this.totalFrames = configuration.getTotalFrames();
        rawFrameSum = new byte[2 * this.totalFrames * 8];
        readMapFile("frameSum", rawFrameSum);
        this.frameSum = ByteBuffer.wrap(rawFrameSum).order(ByteOrder.LITTLE_ENDIAN);
    }
    
    /**
     * Read map data from 
     * 
     * @param dqmap Dynamic map data
     * @param dqmapDim X and Y dimensions of dmap 
     * @param sqmap
     * @param sqmapDim
     */
    public FrameSum(byte[] data, int frames) throws Exception {
        if (data == null) {
            throw new Exception("Invalid flatfield data");
        }
        
        logger.debug("Initializing frame sum data using total frames:" + frames);
        this.totalFrames = frames;
        this.rawFrameSum = data;
        this.frameSum = ByteBuffer.wrap(this.rawFrameSum).order(ByteOrder.LITTLE_ENDIAN);

    }

    public void writeHDFS(String path, Configuration conf) 
            throws IOException {
        write(new Path(path), conf, this.rawFrameSum);
    }
    
    private void write(Path file, Configuration conf, byte[] data) 
            throws IOException {
        FileSystem fs = file.getFileSystem(conf);
        OutputStream outStream = fs.create(file, true, 2 * 1024 * 1024);
        outStream.write(data);
        outStream.flush();
        outStream.close();
        
        fs.close();
    } 
    
    private void readMapFile(String path, byte [] buffer) 
            throws IOException {
        logger.info("Loading map from file " + path);
        File f = new File(path);
        if (! f.exists()) {
            logger.error("Map file not found " + f.getAbsolutePath());
            return;
        }
        
        BufferedInputStream buffin = new BufferedInputStream(
                new FileInputStream(f), 2 * 1024 * 1024);
        int size = buffin.read(buffer);
        buffin.close();
        logger.info("Loaded map : file: " + path + " bytes : " + size);
    }
    
    public double[] getFrameSum() {
    	double[] framesum = new double[this.totalFrames];
        // Skip the first 'row' of the frame sum array. The first row
        // only consists of frame indices. 
    	this.frameSum.rewind().position(this.totalFrames * 8);
    	int cnt = 0;
    	while (this.frameSum.hasRemaining()) {
    		framesum[cnt++] = this.frameSum.getDouble();
    	}

    	return framesum;
    }    
}
