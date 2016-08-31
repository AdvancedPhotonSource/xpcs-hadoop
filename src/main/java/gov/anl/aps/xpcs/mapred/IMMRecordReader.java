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
import gov.anl.aps.xpcs.mapred.filter.FilterMain;
import gov.anl.aps.xpcs.mapred.io.Frame;
import gov.anl.aps.xpcs.mapred.io.FrameValue;
import gov.anl.aps.xpcs.mapred.io.PixelKey;
import gov.anl.aps.xpcs.mapred.io.PixelValue;
import gov.anl.aps.xpcs.util.QMaps;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class IMMRecordReader 
    extends Configured implements RecordReader<PixelKey, PixelValue> {
    
    protected static final int BYTES_PER_PIXEL_VALUE = 2;
    
    protected static final int BYTES_PER_PIXEL_INDEX = 4;
    
    protected static final int BYTES_PER_PIXEL = BYTES_PER_PIXEL_VALUE
            + BYTES_PER_PIXEL_INDEX;

   /**
    * Reporter instance passed by the MR framework.
    */
   protected Reporter reporter = null;
   
   /**
    * Multiple output collector for recording frame-sums separately
    */
   protected MultipleOutputs mos = null;
    
    /**
     * Represent the data stream for input file split.
     */
    protected FSDataInputStream fileIn = null;
    
    /**
     * Job configuration
     */
    protected XPCSConfig configuration = null;
    
    /** 
     * Starting byte in the input split.
     */
    protected long firstByte = 0;
    
    /**
     * Ending byte in the input split.
     */
    protected long lastByte = 0;
    
    /**
     * Width of XPCS image frame
     */
    protected int frameWidth;
    
    /**
     * Height of XPCS image frame
     */
    protected int frameHeight = 0;
    
    /**
     * Detector type
     */
    protected int cameraType = 0;
    
    /**
     * Keep track of current IMM frame being processed.
     */
    protected Header currentHeader;
    
   /**
    * Total number of pixels in current frame.
    */
   protected int pixelCounts = 0;
   
   /**
    * Index of current frame
    */
   protected int frameIndex;
   
   /**
    * Index of current pixel.
    */
   protected int currentPixelCount;
       
   /**
    * Key object to implement createKey() method 
    */
   protected PixelKey pixelKey = new PixelKey();
   
   /**
    * Value object to implement createValue() method
    */
   protected PixelValue pixelValue = new PixelValue();
    
    /**
     * First frame that is part of analysis 
     */
    protected int frameStart = 0;
    
    /**
     * Last frame that is part of analysis
     */
    protected int frameEnd = 0;
    
    /**
     * Is valid frame included in the file split
     */
    protected boolean isValidFrameInSplit = false;
//   
//    /**
//     * Sum up individual frame pixel values and emit as a result.
//     */
//    protected long frameSum = 0;

    /**
     * A output collector to output frame sum and frame time.
     */
    protected OutputCollector frameOutput = null;
    
    /**
     * Kinetics mode beginning slice to process.  
     */
    protected int firstSlice = 0;
    
    /**
     * Kinetics mode last slie to process
     */
    protected int lastSlice = 0;
    
    /**
     * Kinetics mode slice height in #of rows.
     */
    protected int sliceHeight = 0;
    
    /**
     * Kinetics mode slice starting row #.
     */
    protected int sliceTop = 0;

    protected int slicesPerFrame = 0;
    
    /**
     * Frame info value
     */
    protected FrameValue frameValue = new FrameValue();

    private ImageFormat fileFormat = null;

    private boolean hasFrames = true;

    private int[] pixels = null;

    private float[] values = null;
    
    private Frame frame = null;
    
    private int pixelIndex = 0;

    private float pixelIntensity = 0;
    
    private FilterMain filters = null;
    
    private int frameWidthBin = 0;
    
    private int frameHeightBin = 0;

    /**
     * 
     * @param split
     * @param job
     * @param reporter
     * @throws IOException
     */    
    public IMMRecordReader(InputSplit split, JobConf job, Reporter reporter) 
            throws IOException {
		setConf(job);
		this.configuration = new XPCSConfig(job);
		this.filters = new FilterMain(this.configuration);
		this.reporter = reporter;
		this.frameWidth = configuration.getFrameWidth();
		this.frameWidthBin = configuration.getFrameWidth() / (int) configuration.getBinX();
		this.frameHeight = configuration.getFrameHeight();
		this.frameHeightBin = configuration.getFrameHeight() / (int) configuration.getBinY();
		this.cameraType = configuration.getCameraType();
		this.frameStart = configuration.getFirstFrame();
		this.frameEnd = configuration.getLastFrame();

        this.firstSlice = configuration.getFirstSlice();
		this.lastSlice = configuration.getLastSlice();
		this.sliceHeight = configuration.getSliceHeight();
		this.sliceTop = configuration.getSliceTop();
        this.slicesPerFrame = this.lastSlice - this.firstSlice + 1;

        if (this.configuration.getIsSparse()) {
            this.fileFormat = new SparseImageFormat(this.configuration);    
        } else if (this.configuration.getIsKinetics()){
            this.fileFormat = new KineticsImageFormat(this.configuration);
            this.frameEnd = (this.frameEnd - this.frameStart + 1) * this.slicesPerFrame;
            this.frameHeight = this.sliceHeight;
        } else {
        	this.fileFormat = new NonSparseImageFormat(this.configuration);
        }

        List<String> namedOutputs = MultipleOutputs.getNamedOutputsList(job);
        if (namedOutputs.contains("frames"))
        {
            mos = new MultipleOutputs(job);
            frameOutput = mos.getCollector("frames", reporter);
        }

		frame = new Frame(this.frameWidth, this.frameHeight, this.frameStart);

		// Read input split
		readSplit(split);

		sync();
    }

    protected void readSplit(InputSplit split) throws IOException {
        if (split == null) return;
        
        FileSplit fileSplit = ((FileSplit) split);
        final Path file = fileSplit.getPath();
        final FileSystem fs = file.getFileSystem(getConf());
        fileIn = fs.open(file);
        firstByte = fileSplit.getStart();
        lastByte = fileSplit.getLength() + firstByte;
        fileIn.seek(firstByte);
    }

    protected void sync() throws IOException {
        try {
            syncToHeader();
            this.hasFrames = this.fileFormat.skip(fileIn, 
                this.frameStart, this.lastByte);
            
            // We are at the top of the frame we want. 
            nextFrame();

        } catch (IOException ioe) {
            this.isValidFrameInSplit = false;
        }
    }
    /**
     * Locate the next marker (consecutive sequence of 12 bytes)
     *  from the beginning of the data block.
     * @throws IOException
     */
    protected void syncToHeader() throws IOException {
        byte value = 0x00;
        int consecutiveBytes = 0;
        while (true) {
            value = fileIn.readByte();
            if (value == (byte) 0xFF) {
                consecutiveBytes++;
                // There was a bug in previous version of this software where not
                //  checking the consecutive count at each iteration could cause
                //  us to skip the header by few bytes depending on the composition of
                //  actual data right next to the header . 
                //  TODO: We will need to make this check more robust. 

                if (consecutiveBytes == 12) {
                    //  We found the header, now seek back to begining of it, as
                    //   magic bytes are located at the end of the header. 
                    fileIn.seek(fileIn.getPos() - 1024);
                    return;
                }
                
            } else {
                consecutiveBytes = 0;
            }
        }
    }
    
    public void nextFrame() throws IOException {
    	if (! this.hasFrames) return;
   	    
        if (fileIn.getPos() >= this.lastByte || 
            frame.getFrameActualIndex() > this.frameEnd) {
            // If we have read a kinetics frame, we need to process it 
            //  fully before giving up the processing for this map.
            if (this.configuration.getIsKinetics()) {
                KineticsImageFormat imf = (KineticsImageFormat) this.fileFormat;
                if (!imf.hasSlicesInFrame())
                {
                    hasFrames = false;
                    return;
                }
            } else {

                hasFrames = false;
                return;
            }

        }

        this.fileFormat.readNextFrame(fileIn, frame);

        if (frame.getFrameActualIndex() > this.frameEnd) {
            hasFrames = false;
            return;
        }

        this.filters.apply(frame);

        emitFrameSum(frame.getFrameSum(), 
                     frame.getFrameIndex(),
				     frame.getFrameClock(),
                     frame.getFrameTick());
        
        pixels = frame.getIndices();
        values = frame.getPixels();
        this.pixelCounts = frame.getPixelCounts();
        currentPixelCount = 0;

        hasFrames = true;
    }

    public boolean next(PixelKey key, PixelValue value) throws IOException {    
        if (! hasFrames)
            return false;

        if (currentPixelCount < this.pixelCounts) {
            pixelIndex = pixels[currentPixelCount];
            pixelIntensity = values[currentPixelCount];
        }  else {
            // currentPixelCount will be reset by this method.
            nextFrame();

            if (hasFrames) {
                pixelIndex = pixels[currentPixelCount];
                pixelIntensity = values[currentPixelCount];
            } else {
                return false;
            }
        }

        currentPixelCount++;

        key.setX((short) (pixelIndex % frameWidthBin));
        key.setY((short) (pixelIndex / frameWidthBin));

        value.setFrameIndex(frame.getFrameIndex());
        value.setPixelIntensity(pixelIntensity);

        return true;
    }

    protected void emitFrameSum(double frameSum,
       int frameIndex, double frameClock, double frameTick) throws IOException {

        // If frame emitter is not set, just return
        if (frameOutput == null) return;

        frameValue.setFrameSum(frameSum);
        frameValue.setFrameClock(frameClock);
        frameValue.setFrameTick(frameTick);
        frameOutput.collect(frameIndex, this.frameValue);
    }
    
    public PixelKey createKey() {
        return pixelKey;
    }

    public PixelValue createValue() {
        return pixelValue;
    }

    public long getPos() throws IOException {
        return this.fileIn.getPos();
    }

    public void close() throws IOException {
        this.fileIn.close();
        if (this.mos != null)
            this.mos.close();
    }

    public float getProgress() throws IOException {
        return (float) (getPos() - firstByte) 
                 / (float) (lastByte - firstByte);
    }

    public Frame getFrame() {
        return this.frame;
    }

}

