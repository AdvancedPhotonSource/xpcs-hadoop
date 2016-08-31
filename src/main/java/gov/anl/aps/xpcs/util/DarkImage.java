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
import gov.anl.aps.xpcs.mapred.Header;
import gov.anl.aps.xpcs.mapred.filter.Binning;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.ShortBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DarkImage {
    
    /**
     * Default logger
     */
    private static final Logger logger = LoggerFactory
            .getLogger(DarkImage.class.getName());
    
    /**
     * Configuration object
     */
    private XPCSConfig config;
    
    /**
     * Total number of pixels (after binning) in the image
     */
    private int imageSize = 0;
    
    /**
     * Average of all dark images computed using reading dark images from
     * the input file from HDFS.
     */
    private double[] darkAvgImage;
    
    /**
     * Standard deviation of all dark images computed using reading dark images
     * from the input file from HDFS. 
     */
    private double[] darkStdImage;
    
    /**
     * Index of first dark frame. 
     */
    private int darkStart = 0;
    
    /**
     * Index of last dark frame.
     */
    private int darkEnd = 0;
    
    /**
     * Total number of bytes in the IMM frame. 
     */
    private int bytesInFrame = 0;
    
    /**
     * Total number of bytes in the IMM image.
     */
    private int bytesInImage = 0;
    
    /**
     * Internal buffer for reading dark image data. 
     */
    private byte[] buffer = null;
    
    /**
     * Threshold for dark subtraction
     */ 
    private float constantThreshold = 0;
    
    /**
     * for dark subtraciton
     */
    private float constantStdDev = 0;
    
    /**
     * Flat-field object
     */
    private FlatField flatfield = null;
        
    /**
     * Size of the image. 
     * @param totalPixels
     * @param numberOfDarkImage
     * @throws IOException 
     */
    private DarkImage(XPCSConfig config) throws IOException {
        this.config = config;
        this.darkStart = config.getFirstDarkFrame();
        this.darkEnd = config.getLastDarkFrame();
        
		this.imageSize = config.getFrameWidth() * config.getFrameHeight();
        
		bytesInImage  = this.imageSize * XPCSConfig.BYTES_PER_PIXEL_VALUE;
        bytesInFrame =  bytesInImage + Header.HEADER_SIZE_IN_BYTES;

        this.constantThreshold = config.getDarkThreshold();
        this.constantStdDev = config.getDarkSigma();
        this.flatfield = this.config.getFlatField();
    }
    
    public static DarkImage initFromIMMFile(XPCSConfig config)
            throws IOException {
        DarkImage darkImage = new DarkImage(config);
        darkImage.computeDarkImageFromHDFS();
        

        return darkImage;
    }
    
    public static DarkImage initFromFiles(XPCSConfig config, String darkAvg,
            String darkStd) throws IOException {
        DarkImage darkImage = new DarkImage(config);
        darkImage.read(darkAvg, darkStd);
        
        return darkImage;
    }
    
    public double[] getDarkAvgImage() {
        return this.darkAvgImage;
    }
    
    public double[] getDarkStdImage() {
        return this.darkStdImage;
    }
    
    public void writeToHDFS(String darkAvg, String darkStd, Configuration conf)
            throws IOException {

        computeDarkImageFromHDFS();

        write(new Path(darkAvg), this.darkAvgImage, conf);
        write(new Path(darkStd), this.darkStdImage, conf);
    }
    
    public void writeToLocal(String darkAvg, String darkStd) throws IOException {
        write(darkAvg, this.darkAvgImage);
        write(darkStd, this.darkStdImage);
    }

    public float darkSubtract(float value, int pixelIndex) {
        if (darkAvgImage == null || darkStdImage == null)
            return value;
        
        double subtractedValue = value - darkAvgImage[pixelIndex];
        subtractedValue = Math.max( subtractedValue, 0);
         
        double threshold = constantThreshold + 
                constantStdDev * darkStdImage[pixelIndex];

        subtractedValue = (subtractedValue <= threshold) ? 0 : subtractedValue;
       
        return (float)subtractedValue;
    }

    private void write(Path darkAvg, double[] data, Configuration conf) throws IOException {
        OutputStream outStream = darkAvg.getFileSystem(conf)
                .create(darkAvg, true, 128 * 1024, (short) 1,
                        (long) 64 * 1024 * 1024);
        BufferedOutputStream s = new BufferedOutputStream(outStream, 3 * 1024 * 1024);
        DataOutputStream dataStream = new DataOutputStream(s);
        
        for (int i=0; i<data.length; i++) {
            dataStream.writeDouble(data[i]);
        }

        dataStream.flush();
        s.flush();
        outStream.flush();
        s.close();
        outStream.close();
        dataStream.close();
    }
    
    private void writeText(String path, double[] data) throws IOException {
        FileOutputStream outstream = new FileOutputStream(path);
        OutputStreamWriter writer = new OutputStreamWriter(outstream);
        
        for (int y = 0; y < this.config.getFrameHeight(); y++) {
            StringBuilder sb = new StringBuilder();
            for(int x = 0; x < this.config.getFrameWidth(); x++) {
                int index = y * this.config.getFrameWidth() + x;
                if (x != 0) {
                    sb.append(XPCSConfig.SEP);
                }
                sb.append(data[index]);
            }
            sb.append(XPCSConfig.NEWLINE);
            writer.write(sb.toString());
        }
        
        writer.close();
        outstream.close();
    }

    private void write(String path, double[] data) throws IOException {
        
        FileOutputStream outStream = new FileOutputStream(new File(path));
        BufferedOutputStream s = new BufferedOutputStream(outStream, 3 * 1024 * 1024);
        DataOutputStream dataStream = new DataOutputStream(s);
        
        for (int i=0; i<data.length; i++) {
            dataStream.writeDouble(data[i]);
        }

        dataStream.flush();
        s.flush();
        outStream.flush();
        s.close();
        outStream.close();
        dataStream.close();
    }

    
    private void read(String avgImg, String stdImg) throws IOException {
    	int size = this.imageSize; // / (xbin * ybin);
        buffer = new byte[size * 8]; // 8 bytes per data point
        this.darkAvgImage = new double[size];
        this.darkStdImage = new double[size];
        
        readDarkData(avgImg, this.darkAvgImage);
        readDarkData(stdImg, this.darkStdImage);
    }
   
    private void readDarkData(String avgImg, double[] data)
            throws IOException {
        File f = new File(avgImg);
        if (! f.exists()) {
            logger.error("Dark image file not found " + f.getAbsolutePath());
            return;
        }
        
        BufferedInputStream buffin = new BufferedInputStream(
                new FileInputStream(f), 1 * 1024 * 1024);
        int read = buffin.read(buffer);
        if (read < (buffer.length)) {
            throw new IOException("Failed to read dark averages");
        }
        // Most of the format used in this software follow little endian. The 
        // major exception is dark averages computed at the time of job submission
        // and read during processing of input file in Hadoop.
        //TODO: Should we try to make dark average data LITTLE_ENDIAN as well. 
        DoubleBuffer values = ByteBuffer.wrap(buffer)
                .order(ByteOrder.BIG_ENDIAN).asDoubleBuffer();
        
        //TODO sanity checks. Here we should check how many dark image bytes
        // we read. If they are less than the total number of pixels in the data
        // complain!
        int index = 0;
        while (values.hasRemaining()) {
            data[index++] = values.get();
        }
        
        buffin.close();
    }
    
    private int computeSyncMarker() {
        return darkStart * bytesInFrame;
    }
    
    private ShortBuffer readImage(FSDataInputStream fileIn) 
            throws IOException {
        fileIn.readFully(this.buffer);
        return ByteBuffer.wrap(buffer, Header.HEADER_SIZE_IN_BYTES, bytesInImage)
                .order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();
    }
    
    private void computeDarkImageFromHDFS( )
            throws IOException {

        darkAvgImage = new double[this.imageSize];
        darkStdImage = new double[this.imageSize];
        
        Path inputPath = new Path(this.config.getInputFilePath());
        FileSystem hdfs = inputPath.getFileSystem(this.config);
        FSDataInputStream fileIn = hdfs.open(inputPath);
        
        long syncMarker = computeSyncMarker();
        buffer = new byte[bytesInFrame];

        // Sync to first dark frame in the file.
        if (syncMarker > 0) fileIn.seek(syncMarker);
        
        double flatfieldImage[] = null; 
        if (this.config.getIsFlatFieldEnabled()) {
            flatfieldImage = this.flatfield.getFlatField();
        }

        int samples = 1;
        // darkStart index starts from 1, thats why we have <= in the loop. 
        for (int i = darkStart; i <= darkEnd; i++) {
            ShortBuffer image = readImage(fileIn);
            float newimage[] = toFloatImage(image);
            
			if (flatfieldImage != null) {
				FlatField.applyFlatField(newimage, newimage.length,
						flatfieldImage);
			}
//
//            if (xbin > 1 || ybin > 1) {
//				Binning.bin(newimage, config.getFrameWidth(),
//						config.getFrameHeight(), xbin, ybin);
//            }
            
            int index = 0;
            int imageLength = newimage.length; 
            while (index < imageLength) {
                double temp = darkAvgImage[index];
                darkAvgImage[index] += mean(darkAvgImage[index], newimage[index], samples);
                darkStdImage[index] += std(newimage[index], darkAvgImage[index], temp);
                
                if (i == darkEnd) {
                    darkStdImage[index] = Math.sqrt(darkStdImage[index]
                            / samples);
                }
                index++;
            }
            samples++;
        }
    }
    
    private double mean(double mean, double data, double samples) {
        return ( (data - mean) / (samples) );
    }
    
    private double std(double data, double avg1, double avg2) {
        return  ( (data - avg2) * (data - avg1) ) ;
    }
    
    private float[] toFloatImage(ShortBuffer buffer) {
    	float[] image = new float[buffer.remaining()];
    	
    	int cnt = 0;
    	while (buffer.hasRemaining()) {
    		image[cnt++] = buffer.get();
    	}
    	
    	return image;
    }
}
