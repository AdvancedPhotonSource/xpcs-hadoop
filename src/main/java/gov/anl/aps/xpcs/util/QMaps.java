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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QMaps {
    
    private static final Logger logger = LoggerFactory.getLogger(QMaps.class
            .getName());
     /**
     * Wrap buffer data for dynamic partition map
     */
    private ByteBuffer dynamicMap = null;
    
    /**
     * Wrap buffer data for static partition map
     */
    private ByteBuffer staticMap = null;
    
    /**
     * Size of dimension 1, usually the map width
     */
    private int width = 0; 
    
    /**
     * Size of dimensions 2, usually the map height
     */
    private int height = 0;
    
    /**
     * Total number of pixels in the partitions. (width * height)
     * 
     */
    private long totalPixels = 0;
    
    /**
     * Pixel to static partitions mappings 
     */
    private short[][] staticMapping = null;
    
    /**
     * Pixels to dynamic partitions mappings
     */
    private short[][] dynamicMapping = null;
    
    /**
     * Count of pixels in each static partition
     */
    private int[] staticCounts = null;
    
    /**
     * Count of pixels in each dynamic partition
     */
    private int[] dynamicCounts = null;
    
    /**
     * Total number of static partitions
     */
    private int totalStaticPartitions = 0;
    
    /**
     * Total number of dynamic partitions 
     */
    private int totalDynamicPartitions = 0;
    
    /**
     * Maximum number of pixels in the largest dynamic bin. 
     */
    private int maxPixelCountsDynamic = 0;
    
    /**
     * Maximum number of pixels in the largest static bin. 
     */
    
    /**
     * Initialize maps using binary input files. These files will be read and
     * loaded into memory.  
     *  
     * @param dqmapFile Path to binary dynamic partition file
     * @param sqmapFile Path to binary Static partition file
     * @throws IOException 
     */
	public QMaps(XPCSConfig configuration) throws IOException {
        this.width = configuration.getFrameWidth() / (int) configuration.getBinX();
        this.height = configuration.getFrameHeight() / (int)configuration.getBinY();
        
        this.totalPixels = (long) this.width * (long) this.height;
        this.totalDynamicPartitions = configuration.getTotalDynamicPartitions();
        this.totalStaticPartitions = configuration.getTotalStaticPartitions();
        
        byte[] buffer1 = new byte[this.width * this.height * 4];
        byte[] buffer2 = new byte[this.width * this.height * 4];
        
        readMapFile("dqmap", buffer1);
        this.dynamicMap = ByteBuffer.wrap(buffer1).order(ByteOrder.LITTLE_ENDIAN);
        
        readMapFile("sqmap", buffer2);
        this.staticMap = ByteBuffer.wrap(buffer2).order(ByteOrder.LITTLE_ENDIAN);
    }
    
    /**
     * Read map data from 
     * 
     * @param dqmap Dynamic map data
     * @param dqmapDim X and Y dimensions of dmap 
     * @param sqmap
     * @param sqmapDim
     */
    public QMaps(byte[] dqmap, byte[] sqmap, int width, int height) throws Exception {
        if (dqmap == null || sqmap == null) {
            throw new Exception("Invalid map data");
        }
        
        logger.debug("Initilaizing map data width:" + width +
                " height: " + height);
        dynamicMap = ByteBuffer.wrap(dqmap).order(ByteOrder.LITTLE_ENDIAN);
        staticMap = ByteBuffer.wrap(sqmap).order(ByteOrder.LITTLE_ENDIAN);
        this.width = width;
        this.height = height;
        this.totalPixels = (long) this.width * (long) this.height;
    }
    
    /**
     * Outputs partition map data in binary format.
     * 
     * @param qmap
     * @param smap
     * @throws IOException 
     */
    public void writeLocal(String qmap, String smap) throws IOException {
        write(dynamicMap.array(), qmap);
        write(staticMap.array(), smap);
    }
    
    /**
     * Outputs map data as a textual representations
     * 
     * @param filename Location of the output file. 
     * @throws IOException 
     */
    public void writeLocalASCII(String filename) throws IOException {
        write(dynamicMap, staticMap, filename, true);
    }
    
    /**
     * Write map data in binary format to HDFS
     * 
     * @param qmap HDFS path for dynamic partition 
     * @param smap HDFS path for static partition
     * @throws IOException 
     */
    public void writeHDFS(String qmap, String smap, Configuration conf) 
            throws IOException {

        write(new Path(qmap), conf, dynamicMap.array());
        write(new Path(smap), conf, staticMap.array());
    }
    
    /**
     * Return list of pixels that are not part of any partition. 
     * 
     * @return List of pixel indices
     */
    public HashSet<Integer> getPixelMask() {
        HashSet<Integer> mask = new HashSet<Integer>();

        if (this.dynamicMap == null || this.staticMap == null) return null;
        
        this.dynamicMap.rewind();
        this.staticMap.rewind();
        
        int index = 0;
        
        while (this.dynamicMap.hasRemaining() && 
                this.staticMap.hasRemaining()) {
            
            int qvalue = this.dynamicMap.getInt();
            int svalue = this.staticMap.getInt();
            
            if (qvalue < 1 || svalue < 1) {
                mask.add(index);
            }
            
            index++;
        }
        
        return mask;
    }
    /**
     * Return (or build if necessary) pixel mappings between individual
     * pixel and static partitions. 
     * 
     * @return A 2D array with first axis is image column # and second axis is
     *         the image row #. The value is the partition. 
     */
    public short[][] getStaticMapping() {
        buildMaps(false);
        
        return this.staticMapping;
    }
    
    /**
     * Return (or build if necessary) pixel mappings between individual
     * pixel and static partitions. 
     * 
     * @return A 2D array with first axis is image column # and second axis is
     *         the image row #. The value is the partition. 
     */

    public short[][] getDynamicMapping() {
        buildMaps(false);
        
        return this.dynamicMapping;
    }
    
    /**
     * Number of pixels in each static partition. 
     * @return An array of counts with dynamic partition number as index. 
     */
    public int[] getStaticCounts() {
        buildCounts(false);
        
        return this.staticCounts;
    }
    
    /**
     * Number of pixels in each dynamic partition. 
     * @return An array of counts with dynamic partition number as index. 
     */
    public int[] getDynamicCounts() {
        buildCounts(false);
        
        return this.dynamicCounts;
    }
    
    public int getTotalStaticPartitions() {
        buildCounts2(false);
        
        return this.totalStaticPartitions;
    }
    
    public void setTotalStaticPartitions(int v) {
        this.totalStaticPartitions = v;
    }
    
    public int getTotalDynamicPartitions() {
        buildCounts2(false);
        
        return this.totalDynamicPartitions;
    }
    
    public void setTotalDynamicPartitions(int v) {
        this.totalDynamicPartitions = v;
    }
    
    private void buildCounts(boolean rebuild) {
        if (staticCounts != null && !rebuild)
            return;
        
        if (this.totalDynamicPartitions == 0 || this.totalStaticPartitions == 0)
            buildCounts2(rebuild);
        
        staticCounts = new int[this.totalStaticPartitions];
        dynamicCounts = new int[this.totalDynamicPartitions];

        this.staticMap.rewind();
        this.dynamicMap.rewind();
        while (this.staticMap.hasRemaining() &&
                this.dynamicMap.hasRemaining()) {
            int smap = this.staticMap.getInt();
            int dmap = this.dynamicMap.getInt();
            
            if (smap > 0) {
                staticCounts[smap]++;
            }
            
            if (dmap > 0 ) {
                dynamicCounts[dmap]++;
                // Update the maximum number of pixels in the largest dynamic bin. 
                if (dynamicCounts[dmap] > this.maxPixelCountsDynamic)
                    this.maxPixelCountsDynamic = dynamicCounts[dmap];
            }
        }
    }
    
    
    private void buildMaps(boolean rebuild) {
        
        // Unless rebuild is specifically request, don't rebuild the map
        if (staticMapping != null && dynamicMap != null && !rebuild) 
            return;
        
        staticMapping = new short[this.width][this.height];
        dynamicMapping = new short[this.width][this.height];

        this.staticMap.rewind();
        this.dynamicMap.rewind();
        
        int index = 0;
        while (this.staticMap.hasRemaining() &&
                this.dynamicMap.hasRemaining()) {
            int x = index % this.width;
            int y = index / this.width;
            staticMapping[x][y] = (short) staticMap.getInt();
            dynamicMapping[x][y] = (short) dynamicMap.getInt();
            index++;
        }
    }
    
    private void buildCounts2(boolean rebuild) {
        if ( this.totalDynamicPartitions != 0 && this.totalStaticPartitions != 0  
             && !rebuild)
             return;
        
        this.staticMap.rewind();
        this.dynamicMap.rewind();
        while (this.staticMap.hasRemaining() &&
                this.dynamicMap.hasRemaining()) {
            int smap = this.staticMap.getInt();
            int dmap = this.dynamicMap.getInt();
            
            // Since static/dynamic partition numbers are linearly increasing
            // , the max partition number refer to total number of partitions.  
            if (dmap > this.totalDynamicPartitions)
                this.totalDynamicPartitions = dmap;
            
            if (smap > this.totalStaticPartitions)
                this.totalStaticPartitions = smap;
        }
        
        // Static and dynamic partitions starts at index one, the
        // +1 below is to handle this part. 
        this.totalDynamicPartitions += 1;
        this.totalStaticPartitions += 1;
    }
    
    private File getFile(String path, boolean overwrite) throws IOException {
        File f = new File(path);
        if (f.exists() && !overwrite)
            throw new IOException("File already exists " + path);
        
        return f;
    }
    
    private void readMapFile(String mapFile, byte [] buffer) 
            throws IOException {
        logger.info("Loading map from file " + mapFile);
        File f = new File(mapFile);
        if (! f.exists()) {
            logger.error("Map file not found " + f.getAbsolutePath());
            return;
        }
        
        BufferedInputStream buffin = new BufferedInputStream(
                new FileInputStream(f), 1 * 1024 * 1024);
        int size = buffin.read(buffer);
        buffin.close();
        logger.info("Loaded map : file: " + mapFile + " bytes : " + size);
    }
    
    private void write(byte[] map, String path) throws IOException {
        File f = getFile(path, true);
        BufferedOutputStream buffout = new BufferedOutputStream(
                new FileOutputStream(f));
        buffout.write(map);
        buffout.close();
    }
    
    private void write(ByteBuffer qmap, ByteBuffer smap, String path, 
            boolean overwrite) throws IOException {
        if (qmap == null || smap == null || path == null) return;
        
        if (width == 0 || height == 0) {
            logger.debug("Partition width/height information is unknown.");
            return;
        }
        
        File f = getFile(path, overwrite);
        BufferedWriter buffout = new BufferedWriter(new FileWriter(f));

        qmap.rewind();
        smap.rewind();
        int index = 0;
        while (qmap.hasRemaining()) {
            int dq = qmap.getInt();
            int sq = smap.getInt();
            if (sq > 0 && dq > 0) {
                int x = index % width;
                int y = index / width;
                // TODO Don't concatenate strings use string builder
                buffout.write(index + "(" + x + "," + y + ")," + sq + "," + dq + "\n");
            }
            index++;
        }
        
        buffout.flush();
        buffout.close();
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

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }
    
    /**
     * Total number of pixels in the largest dynamic bin based on 
     *  total number of pixels in any given dynamic bin.
     *   
     * @return Number of pixels in largest dynamic bin
     */
    public int getMaxPixelCountsDynamic() {
        if (this.maxPixelCountsDynamic == 0)
            buildCounts(false);
        
        return this.maxPixelCountsDynamic;
    }
    
    public void setMaxPixelCountsDynamic(int value) {
        this.maxPixelCountsDynamic = value;
    }

        
}
