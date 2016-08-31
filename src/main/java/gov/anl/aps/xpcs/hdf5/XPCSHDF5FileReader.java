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

package gov.anl.aps.xpcs.hdf5;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.Datatype;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.HObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XPCSHDF5FileReader {
    /**
     * Instance of logger object. 
     */
    private static final Logger logger = 
            LoggerFactory.getLogger(XPCSHDF5FileReader.class.getName());
    
    /**
     * Path of input HDF5 file containing XPCS parameters 
     */
    private static String inputFile = null;
    
    /**
     * Instance object.
     */
    private static XPCSHDF5FileReader newInstance = null;
    
    /**
     * List of paths that we are interested in reading from the given HDF5 file.
     */
    private static ArrayList<String> paths = new ArrayList<String>();
    
    /**
     * Mapping between HDF5 path and the corresponding value read at that path
     * from the given file. 
     */
    private static HashMap<String, String> valueMap = 
            new HashMap<String, String>();
    
    /**
     * Dynamic partition data from HDF5 file.
     */
    private static byte[] dqmap = null;
    
    /**
     * Dimensions of dynamic partition data.
     */
    private static long dqmapDim[] = null;
    
    /**
     * Static partition data from HDF5 file.
     */
    private static byte[] sqmap = null;
    
    /**
     * Dimensions of static partition data.
     */
    private static long sqmapDim[] = null;
    
    private static byte[] flatfield = null;
    
    /*
     * The normalized frame sum used for the 2-T job.
     */
    private static byte[] frameSum = null;
    
    /*
     * The smoothing image used for the 2-T job.
     */
    private static byte[] pixelSum = null;

    private static long[] pixelSumDims = null;

    /*
     * The normalized pixel sum used for the 2-T job.
     */
    private static byte[] smoothedSG = null;

    private static long[] smoothedSGDims = null;

    /*
     * The end-point within h5 file 
     */
    private String endpoint = null;

    /**
     * QMaps that we are interested in processing for 2-times
    */
    private String qmapsToProcess[] = null;
    
    //TODO: Consider lazy loading of some of the configuration parameters.
    private XPCSHDF5FileReader(String path, String endpoint) throws Exception {
        //TODO Handle native library related errors.
        inputFile = path;
        this.endpoint = endpoint;
        
        buildPaths();
        // Build values-map 
        read(open(inputFile));
    }
    
    private void buildPaths() {       
        paths.add(this.endpoint + XPCSHDF5Const.H5_INT_SWSIZE);
        paths.add(this.endpoint + XPCSHDF5Const.H5_INT_DPL);
        paths.add(this.endpoint + XPCSHDF5Const.H5_INT_FSTART);
        paths.add(this.endpoint + XPCSHDF5Const.H5_INT_FEND);
        paths.add(this.endpoint + XPCSHDF5Const.H5_INT_FSTART_TODO);
        paths.add(this.endpoint + XPCSHDF5Const.H5_INT_FEND_TODO);
        paths.add(this.endpoint + XPCSHDF5Const.H5_INT_DSTART);
        paths.add(this.endpoint + XPCSHDF5Const.H5_INT_DEND);
        paths.add(XPCSHDF5Const.H5_INT_SLICE_Y);
        paths.add(XPCSHDF5Const.H5_INT_SLICE_HEIGHT);
        paths.add(XPCSHDF5Const.H5_INT_SLICE_BEGIN);
        paths.add(XPCSHDF5Const.H5_INT_SLICE_END);
        paths.add(XPCSHDF5Const.H5_INT_XDIM);
        paths.add(XPCSHDF5Const.H5_INT_YDIM);
        paths.add(this.endpoint + XPCSHDF5Const.H5_FLOAT_THRESHOLD);
        paths.add(this.endpoint + XPCSHDF5Const.H5_FLOAT_SIGMA);
        paths.add(XPCSHDF5Const.H5_FLOAT_EFFICIENCY);
        paths.add(XPCSHDF5Const.H5_FLOAT_ADHUPPHOT);
        paths.add(XPCSHDF5Const.H5_FLOAT_PRESET);
        paths.add(XPCSHDF5Const.H5_FLOAT_DPIXX);
        paths.add(XPCSHDF5Const.H5_FLOAT_DPIXY);
        paths.add(XPCSHDF5Const.H5_FLOAT_DISTANCE);
        paths.add(XPCSHDF5Const.H5_FLOAT_FLUXINCIDENT);
        paths.add(XPCSHDF5Const.H5_FLOAT_FLUXTRANSMITTED);
        paths.add(XPCSHDF5Const.H5_FLOAT_THICKNESS);
        paths.add(XPCSHDF5Const.H5_FLOAT_RINGCURRENT);
        paths.add(this.endpoint + XPCSHDF5Const.H5_FLOAT_XBIN);
        paths.add(this.endpoint + XPCSHDF5Const.H5_FLOAT_YBIN);
        paths.add(this.endpoint + XPCSHDF5Const.H5_STRING_IMM);
        paths.add(this.endpoint + XPCSHDF5Const.H5_STRING_OUTPUT);
        paths.add(this.endpoint + XPCSHDF5Const.H5_STRING_SPARSE);
        paths.add(this.endpoint + XPCSHDF5Const.H5_STRING_KINETICS);
        // paths.add(XPCSHDF5Const.H5_STRING_DETECTOR);
        paths.add(this.endpoint + XPCSHDF5Const.H5_STRING_OUTPUT_TAG);
        paths.add(this.endpoint + XPCSHDF5Const.H5_STRING_FLATFIELD);      
        paths.add(this.endpoint + XPCSHDF5Const.H5_STRING_ANALYSIS);
        paths.add(this.endpoint + XPCSHDF5Const.H5_INT_PIXEL_FILTER_SUM);
        paths.add(this.endpoint + XPCSHDF5Const.H5_INT_PIXEL_FILTER_STRIDE);
    }
    
    private static FileFormat open(String inputFile) throws Exception {
        FileFormat fileformat = 
                FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF5);
        
        if (fileformat == null) {
            logger.error("Unable to obtain instance of HDF5 reader");
            return null;
        }

        FileFormat inputH5File = fileformat.createInstance(inputFile,
                                                           FileFormat.WRITE);
        if (inputH5File == null) {
            logger.error("Unable to create instance of HDF5 reader");
            return null;
        }

        if (inputH5File.open() == -1) {
            logger.error("Cannot open HDF5 file (Path : " + inputFile);
            return null;
        }
        logger.debug("open() was successful");
        
        return inputH5File;
    }
    
    //TODO: Refactor: This method can be made generic for all the 2D maps
    private void read(FileFormat hdf5File) throws Exception {
        if (hdf5File == null) {
            logger.debug("FileFormat object is null");
            return;
        }
        
        for (String path : paths) {
            logger.debug("Reading scalar value at path " + path);
            String value = readScalar(hdf5File, path);
            logger.debug(path + " -> " + value);
            if (value != null) {
                valueMap.put(path, value);
            }
        }
        
        // Reading dynamic qmap
        logger.debug("Read map file " + this.endpoint + XPCSHDF5Const.H5_2D_DQMAP);
        Dataset ds = readMap(hdf5File, this.endpoint + XPCSHDF5Const.H5_2D_DQMAP);
        if (ds != null) {
            dqmap = ds.readBytes();
            dqmapDim = ds.getDims();
        }
        
        logger.debug("Reading map file " + this.endpoint + XPCSHDF5Const.H5_2D_SQMAP);
        ds = readMap(hdf5File, this.endpoint + XPCSHDF5Const.H5_2D_SQMAP);
        if (ds != null) {
            sqmap = ds.readBytes();
            sqmapDim = ds.getDims();
        }
        
        try {
        	ds = readMap(hdf5File, XPCSHDF5Const.H5_2D_FLATFIELD);
        	if (ds != null) {
        		flatfield = ds.readBytes();        		
        	}
        } catch (Exception e) {e.printStackTrace();}
        
        try {
        	ds = readMap(hdf5File, this.endpoint + XPCSHDF5Const.H5_2D_FRAMESUM);
        	if (ds != null)
        		frameSum = ds.readBytes();
        } catch(Exception e){ frameSum = null;}
        
        try {
            String[] res = readList(hdf5File, this.endpoint + XPCSHDF5Const.H5_1D_BINTOPROCESS);
            if (res != null) this.qmapsToProcess = res;
        } catch (Exception e) {
        }

        try {
            ds = readMap(hdf5File, this.endpoint + XPCSHDF5Const.H5_2D_PIXELSUM);
            if (ds != null) {
                this.pixelSum = ds.readBytes();
                pixelSumDims = ds.getDims();
            }
        } catch (Exception e) {}

        try {
            ds = readMap(hdf5File, this.endpoint + XPCSHDF5Const.H5_2D_SG);
            if (ds != null) {
                this.smoothedSG = ds.readBytes();
                this.smoothedSGDims = ds.getDims();
            }
        } catch (Exception e) {}

        hdf5File.close();
    }
    
    //TODO: Most of these values should be read in their respective data-type
    // and not string.
    private static String readScalar(FileFormat hdf5File, String path) 
            throws Exception {
        String result = null;

        if (hdf5File == null || path == null)
            return null;

        HObject h5obj = hdf5File.get(path);
        if (h5obj == null || !(h5obj instanceof Dataset)) {
            logger.warn("HDF5 dataset path does not exist or is not valid. "
                        + path);
            return null;
        }

        Dataset dataset = (Dataset) h5obj;
        Datatype dataType = dataset.getDatatype();
        int dType = dataType.getDatatypeClass();

        if (dType == Datatype.CLASS_STRING) {
            logger.debug(path + " DataType = String");
            String[] stringObj = (String[]) dataset.read();
            if (stringObj != null && stringObj.length > 0) {
                result = stringObj[0];
            }
        } else if (dType == Datatype.CLASS_FLOAT) {
            logger.debug(path + " DataType = Float");
            switch (dataType.getDatatypeSize()) {
            case 4:
                float[] f = (float[]) dataset.read();
                if (f.length > 0) {
                    result = "" + f[0];
                }
                break;
            case 8:
                double[] d = (double[]) dataset.read();
                if (d.length > 0) {
                    result = "" + d[0];
                }
                break;
            }
        } else if (dType == Datatype.CLASS_INTEGER) {
            logger.debug(path + " DataType = Integer");
            switch (dataType.getDatatypeSize()) {
            case 2:
                short[] dataShort = (short[]) dataset.read();
                if (dataShort.length > 0) {
                    result = "" + dataShort[0];
                }
                break;
            case 4:
                int[] dataInt = (int[]) dataset.read();
                if (dataInt.length > 0) {
                    result = "" + dataInt[0];
                }
                break;
            case 8:
                long[] dataLong = (long[]) dataset.read();
                if (dataLong.length > 0) {
                    result = "" + dataLong[0];
                }
                break;
            }
        } else {
            logger.error("Un-supported data-type for path " + path);
        }

        return result;
    }
    
    private static Dataset readMap(FileFormat inputH5File, String map) 
            throws Exception {

        if (inputH5File == null) {
            logger.debug("Invalid HDF5 object");
            return null;
        }

        logger.info("Reading map files : 1. " + map);
        HObject h5obj = inputH5File.get(map);
        if (h5obj == null || !(h5obj instanceof Dataset)) {
            logger.error("The given HDF5 dataset path does not exist "
                    + map);
            return null;
        }

        Dataset dataset = (Dataset) h5obj;
        
        return dataset;
    }    

    private static String[] readList(FileFormat hdf5File, String path) throws Exception {

        if (hdf5File == null) {
            logger.error("Invalid HDF5 Object while reading " + path);
            return null;
        }

        logger.info("Reading list " + path);
        HObject h5obj = hdf5File.get(path);
        if (h5obj == null || !(h5obj instanceof Dataset)) {
            logger.error("The given HDF5 dataset path is invalid " + path);
            return null;
        }

        Dataset dataset = (Dataset) h5obj;
        
        Datatype dataType = dataset.getDatatype();
        int dType = dataType.getDatatypeClass();

        System.out.println(dType);

        String result[] = null;

        if (dType == Datatype.CLASS_FLOAT)
        {
            double[] values = (double[]) dataset.read();

            result = new String[values.length];
            for (int i = 0 ; i < values.length; i++)
            {
                result[i] = "" + values[i];
            }
        }
        else if (dType == Datatype.CLASS_INTEGER)
        {
            long[] values = (long[]) dataset.read();

            result = new String[values.length];
            for (int i = 0; i < values.length; i++)
            {
                result[i] = "" + values[i];
            }
        }
     
        return result;
    }

    /**
     * A factory method for creating instance of this class.
     * 
     * A new instance is created if one doesn't already exists. 
     * A new instance is created if the passed file is different than the
     *  one read by the existing instance. 
     *  
     * @param path Path to the HDF5 file
     * @return Instance of this class
     * @throws Exception 
     */
    public static XPCSHDF5FileReader getInstance(String path, String endpoint) throws Exception {
        if (newInstance == null) {
            newInstance = new XPCSHDF5FileReader(path, endpoint);
        }
        
//        // If existing input file is different than the requested path.
//        else if ( inputFile != null && !inputFile.equals(path) ) {
//            newInstance = new XPCSHDF5FileReader(path);
//        }
//        
        return newInstance;
    }
    
    /**
     * Returns value at the current HDF5 path.
     * 
     * @param path
     * @return
     * @throws Exception 
     */
    public String getValue(String path) throws Exception {
        if (!valueMap.containsKey(path))
            throw new Exception(path + " does not exist");
        
        return valueMap.get(path);
    }
    
    public byte[] getDynamicMap() {
        return dqmap;
    }
    
    public long[] getDynamicMapDims() {
        return dqmapDim;
    }
    
    public byte[] getStaticMap() {
        return sqmap;
    }
    
    public long[] getStaticMapDims() {
        return sqmapDim;
    }
    
    public byte[] getFlatField() {
    	return this.flatfield;
    }

    public String[] getQMapsToProcess() {
        return this.qmapsToProcess;
    }

    public byte[] getFrameSum() {
        return this.frameSum;
    }

    public byte[] getPixelSum() {
        return this.pixelSum;
    }

    public long[] getPixelSumDims() {
        return this.pixelSumDims;
    }

    public byte[] getSmoothedSG() {
        return this.smoothedSG;
    }

    public long[] getSmoothedSGDims() {
        return this.smoothedSGDims;
    }
}

