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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.anl.aps.xpcs.hdf5.XPCSHDF5Const;
import gov.anl.aps.xpcs.hdf5.XPCSHDF5FileReader;
import gov.anl.aps.xpcs.util.FlatField;
import gov.anl.aps.xpcs.util.QMaps;
import gov.anl.aps.xpcs.util.FrameSum;
import gov.anl.aps.xpcs.util.PixelSum;
import gov.anl.aps.xpcs.util.SmoothedSG;

import java.util.Date;
import java.sql.Timestamp;

public class XPCSHDFConfig {
    
    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory
            .getLogger(XPCSHDFConfig.class.getName());
    
    private XPCSConfig config = null;
    
    private XPCSHDF5FileReader hdf5config = null;
    
    private String endpoint = null;
    
    public XPCSHDFConfig(XPCSConfig config) throws Exception {
        this.config = config;
        this.endpoint = config.getH5Endpoint();
        
        if (! this.endpoint.endsWith("/")) endpoint += "/";
        
        String hdf5File = config.getHDF5ConfigFile();
        hdf5config = XPCSHDF5FileReader.getInstance(hdf5File, this.endpoint);
        load();
    }
    /**
     * Load configuration properties for XPCS from the HDF5.
     *  
     * @param hdf5File Path of XPCS HDF5 file
     * @throws Exception 
     */
    private void load() throws Exception {
        if (config == null  || config.getHDF5ConfigFile() == null) return;
        
        String hdf5File = config.getHDF5ConfigFile();
        logger.info("Loading configurations from HDF5 file " + hdf5File);
        
        XPCSHDF5FileReader hdf5config = XPCSHDF5FileReader
                .getInstance(hdf5File, this.endpoint);

        // Input IMMM file
        this.config.setInputFilePath(hdf5config
                .getValue(endpoint + XPCSHDF5Const.H5_STRING_IMM));

        // Output directory both local and remote.
        // TODO Use separate HDF5 paths for local and remote output dir
        String outputPrefix = hdf5config.getValue(endpoint + XPCSHDF5Const.H5_STRING_OUTPUT);
        // Timestamp ts = new Timestamp((new java.util.Date()).getTime());

        this.config.setOutputDir(outputPrefix + "_" + System.currentTimeMillis());

        // HDF5 Tag to use for storing final results
        this.config.setOutputTag(hdf5config.getValue(
                endpoint + XPCSHDF5Const.H5_STRING_OUTPUT_TAG));

        int dpl = toInt(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_INT_DPL));
        this.config.setDPL(dpl);

        // Frame type (sparse, non-sparse, kinetics ? )
        // sparse
        String param = hdf5config.getValue(endpoint + XPCSHDF5Const.H5_STRING_SPARSE);
        if (param != null) {
            boolean flag = param.equals("ENABLED") ? true : false;
            this.config.setIsSparse(flag);
        }
        // Kinetics
        param = hdf5config.getValue(endpoint + XPCSHDF5Const.H5_STRING_KINETICS);
        if (param != null) {
            boolean flag = param.equals("ENABLED") ? true : false;
            this.config.setIsKinetics(flag);
        }

        // static mean window size;
        if (param != null) {
            int window = toInt(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_INT_SWSIZE));
            this.config.setStaticWindow(window);
        }
        
        if (this.config.getIsNonSparse() || this.config.getIsKinetics()) {
            configureNonSparseModeParams();
        }
        
        if (this.config.getIsKinetics()) {
            configureKineticsModeParams();
        }

        // Total number of frames
        int fstart = toInt(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_INT_FSTART));
        int fend = toInt(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_INT_FEND));
        int fstartTodo = toInt(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_INT_FSTART_TODO));
        int fendTodo = toInt(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_INT_FEND_TODO));
        int w = toInt(hdf5config.getValue(XPCSHDF5Const.H5_INT_XDIM));
        int h = toInt(hdf5config.getValue(XPCSHDF5Const.H5_INT_YDIM));
        
        
        this.config.setFramecount(fendTodo - fstartTodo + 1);
        // Frame index in HDF5 file start with 1 instead of 0.
        this.config.setFirstFrame(fstartTodo - 1);
        this.config.setLastFrame(fendTodo - 1);
        this.config.setTotalFrames(fend - fstart + 1);
        this.config.setFrameWidth(w);
        this.config.setFrameHeight(h);
        
        param = hdf5config.getValue(endpoint + XPCSHDF5Const.H5_STRING_ANALYSIS);

        if (param != null && config.getAnalysisType() == XPCSConfig.ANALYSIS_UNDEF)
        {
            if (param.equalsIgnoreCase("twotime"))
                this.config.setAnalysisType(XPCSConfig.ANALYSIS_TWOTIMES);
            else
                this.config.setAnalysisType(XPCSConfig.ANALYSIS_MULTITAU);
        }
        
        if (this.config.getIsTwoTimes()) {
            configureTwoTimesParams();
        }
        // Configure partition map
        configureQMaps();

        // Read camera type
        // configureCamerType();
        
        // Read detector parameters for things like normalization factors, time
        configureDetectorParams();

        // Read xbin and ybin values
        // Legacy hdf5 file won't have binX or binY values, Make sure we handle them here
        float binX = 1.0f;
        float binY = 1.0f;
        try  {
            binX = toFloat(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_FLOAT_XBIN));
            binY = toFloat(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_FLOAT_YBIN));
        } catch (Exception e) {}
        finally {
            this.config.setBinX(binX);
            this.config.setBinY(binY);
        }
        
        // Flat field enabled or not
        param = hdf5config.getValue(endpoint + XPCSHDF5Const.H5_STRING_FLATFIELD);
        if (param != null) {
            boolean flag = param.equals("ENABLED") ? true : false;
            this.config.setIsFlatFieldEnabled(flag);
        }
        
        byte[] field = hdf5config.getFlatField();
        if (field != null) {
        	FlatField f = new FlatField(field, w, h);
        	this.config.setFlatField(f);
        }

        int pixelFilterSum = 1;
        int pixelFilterStride = 1;

        try {
            pixelFilterSum  = toInt(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_INT_PIXEL_FILTER_SUM));
            pixelFilterStride = toInt(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_INT_PIXEL_FILTER_STRIDE));
        } catch (Exception e) {}
        finally {
            this.config.setPixelFilterSum(pixelFilterSum);
            this.config.setPixelFilterStride(pixelFilterStride);
        }
        
    }
    
    private void configureNonSparseModeParams() throws Exception {
        
        // Total number of dark frames
        int dfirst = toInt(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_INT_DSTART));
        int dend = toInt(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_INT_DEND));
        
        this.config.setDarkFrames(dend - dfirst + 1);
        this.config.setFirstDarkFrame(dfirst - 1);
        this.config.setLastDarkFrame(dend - 1);
        
        float value = toFloat(hdf5config
                .getValue(endpoint + XPCSHDF5Const.H5_FLOAT_THRESHOLD));
        this.config.setDarkThreshold(value);
        value = toFloat(hdf5config.getValue(endpoint + XPCSHDF5Const.H5_FLOAT_SIGMA));
        this.config.setDarkSigma(value);

    }
    
    private void configureQMaps() {
        if (hdf5config == null) return;
        
        byte[] dmap = hdf5config.getDynamicMap();
        byte[] smap = hdf5config.getStaticMap();
        long[] dmapDims = hdf5config.getDynamicMapDims();
        long[] smapDims = hdf5config.getStaticMapDims();
        
        if (dmapDims == null || smapDims == null ) return;
        if (dmap == null || smap == null ) return;

        if (dmapDims.length != 2) return;
        if (smapDims.length != 2) return;
        if (dmapDims[0] != smapDims[0]) return;
        if (smapDims[1] != smapDims[1]) return;
        
        try {
            QMaps maps = new QMaps(dmap, smap, (int) dmapDims[1], 
                    (int) dmapDims[0]);
            this.config.setQMap(maps);
            this.config.setTotalStaticPartitions(maps.getTotalStaticPartitions());
            this.config.setTotalDynamicPartitions(maps.getTotalDynamicPartitions());
            this.config.setMaxPixelCountsDynamic(maps.getMaxPixelCountsDynamic());
            
        } catch (Exception e) {
            logger.error(e.getMessage());
            logger.debug(e.getStackTrace().toString());
        }
    }
    
    // private void configureCamerType() throws Exception {
    //     String cameraType = hdf5config.getValue(
    //     		XPCSHDF5Const.H5_STRING_DETECTOR);
        
    //     int cameraNumber = 8;
        
    //     if (cameraType.contains("ANL-LBL Fast CCD Detector")) {
    //         cameraNumber = 15;
    //     }
    //     else if (cameraType.contains("DALSA")) {
    //         cameraNumber = 5;
    //     }
    //     else if (cameraType.contains("SMD")) {
    //         cameraNumber =  6;
    //     }
    //     else if (cameraType.contains("PI Princeton Instruments")) {
    //         cameraNumber =  8;
    //     }
        
    //     this.config.setCameraType(cameraNumber);
    // }
    
    private void configureDetectorParams() throws Exception {
        if (hdf5config == null) return;
        
        float value = toFloat(hdf5config.getValue(
        		XPCSHDF5Const.H5_FLOAT_EFFICIENCY));
        this.config.setDetEfficiency(value);
        
        value = toFloat(hdf5config.getValue(XPCSHDF5Const.H5_FLOAT_ADHUPPHOT));
        this.config.setDetAdhupPhot(value);
        
        value = toFloat(hdf5config.getValue(XPCSHDF5Const.H5_FLOAT_PRESET));
        this.config.setDetPreset(value);
        
        value = toFloat(hdf5config.getValue(XPCSHDF5Const.H5_FLOAT_DPIXX));
        this.config.setDetDpixX(value);
        
        value = toFloat(hdf5config.getValue(XPCSHDF5Const.H5_FLOAT_DPIXY));
        this.config.setDetDpixY(value);
        
        value = toFloat(hdf5config.getValue(XPCSHDF5Const.H5_FLOAT_DISTANCE));
        this.config.setDetDistance(value);
        
        value = toFloat(hdf5config.getValue(
                XPCSHDF5Const.H5_FLOAT_FLUXINCIDENT));
        this.config.setDetFluxIncidnet(value);
        
        value = toFloat(hdf5config.getValue
                (XPCSHDF5Const.H5_FLOAT_FLUXTRANSMITTED));
        this.config.setDetFluxTransmitted(value);
        
        try {
            value = toFloat(hdf5config.getValue(
                    XPCSHDF5Const.H5_FLOAT_RINGCURRENT));
            this.config.setDetRingCurrent(value);
        } catch (Exception e) {
            this.config.setDetRingCurrent(100.0f); // default value
        }
        
        value = toFloat(hdf5config.getValue(XPCSHDF5Const.H5_FLOAT_THICKNESS));
        this.config.setDetThickness(value);
        
    }
    
    private void configureKineticsModeParams() throws Exception {
        int value = toInt(hdf5config.getValue(XPCSHDF5Const.H5_INT_SLICE_BEGIN));
        this.config.setFirstSlice(value);
    
        value = toInt(hdf5config.getValue(XPCSHDF5Const.H5_INT_SLICE_END));
        this.config.setLastSlice(value);
        
        value = toInt(hdf5config.getValue(XPCSHDF5Const.H5_INT_SLICE_HEIGHT));
        this.config.setSliceHeight(value);
        
        value = toInt(hdf5config.getValue(XPCSHDF5Const.H5_INT_SLICE_Y));
        this.config.setSliceTop(value);
    }
    
    private void configureTwoTimesParams() throws Exception {
        this.config.setQMapBinsToProcess(hdf5config.getQMapsToProcess());

        byte[] fsum = hdf5config.getFrameSum();
        if (fsum != null) {
            FrameSum f = new FrameSum(fsum, this.config.getTotalFrames());
            this.config.setFrameSum(f);
        }

        byte[] psum = hdf5config.getPixelSum();
        long[] psumDims = hdf5config.getPixelSumDims();

        if (psum != null) {
            PixelSum p = new PixelSum(psum, (int) psumDims[1], (int) psumDims[0]);
            this.config.setPixelSum(p);
        }

        byte[] smoothSG = hdf5config.getSmoothedSG();
        long[] smoothSGDims = hdf5config.getSmoothedSGDims();

        if (smoothSG != null) {
            SmoothedSG sg = new SmoothedSG(smoothSG, (int) smoothSGDims[1], (int) smoothSGDims[0]);
            this.config.setSmoothedSG(sg);
        }
    }

   private int toInt(String value) {
        return Integer.parseInt(value);
   }
   
   private float toFloat(String value) {
       return Float.parseFloat(value);
   }

   private long toLong(String value) {
        return Long.parseLong(value);
   }
}
