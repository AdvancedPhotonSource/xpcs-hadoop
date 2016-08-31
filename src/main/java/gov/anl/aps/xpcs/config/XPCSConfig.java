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

package gov.anl.aps.xpcs.config;

import gov.anl.aps.xpcs.util.FlatField;
import gov.anl.aps.xpcs.util.QMaps;
import gov.anl.aps.xpcs.util.FrameSum;
import gov.anl.aps.xpcs.util.PixelSum;
import gov.anl.aps.xpcs.util.SmoothedSG;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration object for maintaining all configuration 
 * options. 
 *
 */
public class XPCSConfig extends Configuration {

    public static final int BYTES_PER_PIXEL_VALUE = 2;
    
    public static final int BYTES_PER_PIXEL_INDEX = 4;
    
    public static final int BYTES_PER_PIXEL = BYTES_PER_PIXEL_VALUE
            + BYTES_PER_PIXEL_INDEX;
    
    // Pick what type of analysis we need to run.
    public static final int ANALYSIS_UNDEF = 0;

    public static final int ANALYSIS_MULTITAU = 1;
    
    public static final int ANALYSIS_TWOTIMES = 2;
    
    public static final String SEP = ",";
    
    public static final String NEWLINE = "\n";

    /**
     * QMaps object.
     */
    private QMaps qmap = null;
    
    private FlatField flatfield = null;

    private FrameSum frameSum = null;

    private PixelSum pixelSum = null;

    private SmoothedSG smoothedSG = null;
    
    public XPCSConfig(Configuration conf) {
        super(conf);
    }
    
    /**
     * Run a sanity check on configuration parameters.
     * @return
     */
    public boolean verify() {
        throw new NotImplementedException();
    }
    
    /**
     * Get total number of frames to process as specified by hdf5 begin/end 
     * value.
     * @return
     */
    public int getFramecount() {
        return getInt("xpcs.frame.count", 0);
    }

    /**
     * Set total number of frames to process.
     * @param count
     */
    public void setFramecount(int count) {
        setInt("xpcs.frame.count", count);
    }

    /**
     * Get total number of frames in the data as part of the input file.
     */
    public int getTotalFrames() {
        return getInt("xpcs.frame.total", 0);
    }

    /**
    * Set total number of frames in the data as part of the input file.
    */
    public void setTotalFrames(int total) {
        setInt("xpcs.frame.total", total);
    }
    
    /**
     * Path to input IMM file. 
     * @return Path of input IMM file within Hadoop file system. 
     */
    public String getInputFilePath() {
        return get("xpcs.input.imm", "");
    }
    
    /**
     * Set path to IMM file. 
     * @param path
     */
    public void setInputFilePath(String path) {
        set("xpcs.input.imm", path);
    }
    
    public String getOutputDir() {
        return get("xpcs.output.dir", "output");
    }
    
    public void setOutputDir(String output) {
        set("xpcs.output.dir", output);
    }
    
    public boolean getIsSparse() {
        return getBoolean("xpcs.input.sparse", false);
    }
    
    public void setIsSparse(boolean flag) {
        setBoolean("xpcs.input.sparse", flag);
    }
    
    public boolean getIsNonSparse() {
        return ( getIsSparse() != true  && getIsKinetics() != true);
    }
    
    public boolean getIsKinetics() {
        return getBoolean("xpcs.input.kinetics", false);
    }
    
    public void setIsKinetics(boolean flag) {
        setBoolean("xpcs.input.kinetics", flag);
    }
    
    public int getFrameWidth() {
        return getInt("xpcs.frame.width", 0);
    }
    
    public void setFrameWidth(int w) {
        setInt("xpcs.frame.width", w);
    }
    
    public int getFrameHeight() {
        return getInt("xpcs.frame.height", 0);
    }
    
    public void setFrameHeight(int w) {
        setInt("xpcs.frame.height", w);
    }
    
    public int getFirstFrame() {
        return getInt("xpcs.frame.first", 0);
    }
    
    public void setFirstFrame(int f) {
        setInt("xpcs.frame.first", f);
    }
    
    public int getLastFrame() {
        return getInt("xpcs.frame.last", this.getFramecount());
    }
    
    public void setLastFrame(int l) {
        setInt("xpcs.frame.last", l);
    }
    
    public int getFirstDarkFrame() {
        return getInt("xpcs.darkframe.first", 0);
    }
    
    public void setFirstDarkFrame(int d) {
        setInt("xpcs.darkframe.first", d);
    }
    
    public int getLastDarkFrame() {
        return getInt("xpcs.darkframe.last", 0);
    }
    
    public void setLastDarkFrame(int d) {
        setInt("xpcs.darkframe.last", d);
    }
    
    public int getDarkFrames() {
        return getInt("xpcs.darkframe.count", 0);
    }
    
    public void setDarkFrames(int c) {
        setInt("xpcs.darkframe.count", c);
    }
    
    public int getCameraType() {
        return getInt("xpcs.frame.camera", 8);
    }
    
    public void setCameraType(int c) {
        setInt("xpcs.frame.camera", c);
    }

    public String getHDF5ConfigFile() {
        return get("xpcs.config.hdf5");
    }
    
    public void setHDF5ConfigFile(String path) {
        set("xpcs.config.hdf5", path);
    }
    
    public int getDPL() {
        return getInt("xpcs.g2.dpl", 4);
    }
    
    public void setDPL(int dpl) {
        setInt("xpcs.g2.dpl", dpl);
    }
    
    public void setQMap(QMaps qmap) {
        this.qmap = qmap;
    }
    
    public int getStaticWindow() {
        return getInt("xpcs.mean.window", this.getFramecount());
    }
    
    public void setStaticWindow(int w) {
        setInt("xpcs.mean.window", w);
    }
    
    public float getDarkThreshold() {
        return getFloat("xpcs.darkframe.threshold", 0);
    }
    
    public void setDarkThreshold(float v) {
        setFloat("xpcs.darkframe.threshold", v);
    }
    
    public float getDarkSigma() {
        return getFloat("xpcs.darkframe.sigma", 0);
    }
    
    public void setDarkSigma(float v) {
        setFloat("xpcs.darkframe.sigma", v);
    }
    
    public float getDetEfficiency() {
        return getFloat("xpcs.detector.efficiency", 0);
    }
    
    public void setDetEfficiency(float v) {
        setFloat("xpcs.detector.efficiency", v);
    }
    
    public float getDetAdhupPhot() {
        return getFloat("xpcs.detector.adhupphot", 0);
    }
    
    public void setDetAdhupPhot(float v) {
        setFloat("xpcs.detector.adhupphot", v);
    }

    public float getDetPreset() {
        return getFloat("xpcs.detector.preset", 0);
    }
    
    public void setDetPreset(float v) {
        setFloat("xpcs.detector.preset", v);
    }
    
    public float getDetDpixX() {
        return getFloat("xpcs.detector.dpixx", 0);
    }
    
    public void setDetDpixX(float v) {
        setFloat("xpcs.detector.dpixx", v);
    }
   
    public float getDetDpixY() {
        return getFloat("xpcs.detector.dpixy", 0);
    }
    
    public void setDetDpixY(float v) {
        setFloat("xpcs.detector.dpixy", v);
    }
    
    public float getDetDistance() {
        return getFloat("xpcs.detector.distance", 0);
    }
    
    public void setDetDistance(float v) {
        setFloat("xpcs.detector.distance", v);
    }
    
    public float getDetFluxIncidnet() {
        return getFloat("xpcs.detector.fluxincident", 0);
    }
    
    public void setDetFluxIncidnet(float v) {
        setFloat("xpcs.detector.fluxincident", v);
    }
    
    public float getDetFluxTransmitted() {
        return getFloat("xpcs.detector.fluxtransmitted", 0);
    }
    
    public void setDetFluxTransmitted(float v) {
        setFloat("xpcs.detector.fluxtransmitted", v);
    }
    
    public float getDetThickness() {
        return getFloat("xpcs.detector.thickness", 0);
    }
    
    public void setDetThickness(float v) {
        setFloat("xpcs.detector.thickness", v);
    }
    
    public float getDetRingCurrent() {
        return getFloat("xpcs.detector.rcurrent", 0);
    }
    
    public void setDetRingCurrent(float v) {
        setFloat("xpcs.detector.rcurrent", v);
    }
    
    public QMaps getMaps() {
        return this.qmap;
    }
    
    public int getFirstSlice() {
        return getInt("xpcs.kinetics.firstslice", 0);
    }
   
    public void setFirstSlice(int v) {
        setInt("xpcs.kinetics.firstslice", v);
    }
   
    public int getLastSlice() {
        return getInt("xpcs.kinetics.lastslice", 0);
    }
   
    public void setLastSlice(int v) {
        setInt("xpcs.kinetics.lastslice", v);
    }
   
    public int getSliceTop() {
        return getInt("xpcs.kinetics.slicetop", 0);
    }
   
    public void setSliceTop(int v) {
        setInt("xpcs.kinetics.slicetop", v);
    }
   
    public int getSliceHeight() {
        return getInt("xpcs.kinetics.sliceheight", 0);
    }

   public void setSliceHeight(int v) {
       setInt("xpcs.kinetics.sliceheight", v);
   }
   
   public int getTotalStaticPartitions() {
       return getInt("xpcs.qmap.static.count", 0);
   }
   
   public void setTotalStaticPartitions(int v) {
       setInt("xpcs.qmap.static.count", v);
   }
   
   public int getTotalDynamicPartitions() {
       return getInt("xpcs.qmap.dynamic.count", 0);
   }
   
   public void setTotalDynamicPartitions(int v) {
       setInt("xpcs.qmap.dynamic.count", v);
   }
   
   public int getMaxPixelCountsDynamic() {
       return getInt("xpcs.qmap.dynamic.maxpixels", 0);
   }
   
   public void setMaxPixelCountsDynamic(int v) {
       setInt("xpcs.qmap.dynamic.maxpixels", v);
   }

   public String getOutputTag() {
        return get("xpcs.output.tag", "/exchange");
   }

   public void setOutputTag(String v) {
       set("xpcs.output.tag", v);
   }

   public float getBinX() {
       return getFloat("xpcs.frame.binX", 1.0f);
   }

   public void setBinX(float x) {
        setFloat("xpcs.frame.binX", x);
   }

    public float getBinY() {
       return getFloat("xpcs.frame.binY", 1.0f);
   }
   
   public void setBinY(float y) {
        setFloat("xpcs.frame.binY", y);
   }
   
   public boolean getIsFlatFieldEnabled() {
	   return getBoolean("xpcs.frame.flatfield", false);
   }
   
   public void setIsFlatFieldEnabled(boolean flag) {
	   setBoolean("xpcs.frame.flatfield", flag);
   }
   
   public FlatField getFlatField() {
	   return this.flatfield;
   }
   
   public void setFlatField(FlatField f) {
	   this.flatfield = f;
   }

   public FrameSum getFrameSum() {
        return this.frameSum;
   }

   public void setFrameSum(FrameSum f) {
        this.frameSum = f;
   }
   
   public PixelSum getPixelSum() {
        return this.pixelSum;
   }

   public void setPixelSum(PixelSum p) {
        this.pixelSum = p;
   }

   public SmoothedSG getSmoothedSG() {
        return this.smoothedSG;
   }

   public void setSmoothedSG(SmoothedSG sg) {
        this.smoothedSG = sg;
   }

   public int getAnalysisType() {
	   return getInt("xpcs.config.analysis_type", XPCSConfig.ANALYSIS_UNDEF);
   }
   
   public void setAnalysisType(int t) {
	   setInt("xpcs.config.analysis_type", t);
   }
   
   public boolean getIsTwoTimes() {
        return getInt("xpcs.config.analysis_type", XPCSConfig.ANALYSIS_MULTITAU) == XPCSConfig.ANALYSIS_TWOTIMES;
   }
   
   public void setH5Endpoint(String ep) {
	   set("xpcs.config.hdf5.endpoint", ep);
   }
   
   public String getH5Endpoint() {
	   return get("xpcs.config.hdf5.endpoint", "/xpcs");
   }

   public void setQMapBinsToProcess(String [] s)
   {    
        setStrings("xpcs.qmap.binsToProcess", s);
   }

   public String[] getQMapBinsToProcess()
   {
        return getStrings("xpcs.qmap.binsToProcess", null);
   }

   public int getPixelFilterSum()
   {
      return getInt("xpcs.pixel.filter.sum", 1);
   }

   public void setPixelFilterSum(int v)
   {
      setInt("xpcs.pixel.filter.sum", v);
   }

   public int getPixelFilterStride()
   {
       return getInt("xpcs.pixel.filter.stride", 1);
   }

   public void setPixelFilterStride(int v)
   {
       setInt("xpcs.pixel.filter.stride", v);
   }
   
}
