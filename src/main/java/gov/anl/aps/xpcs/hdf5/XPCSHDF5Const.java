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

/**
 * HDF5 file paths corresponding to XPCS configuration.
 */
public class XPCSHDF5Const {
    /**
     * IMM file as input to the analysis, the data-type is string.
     */
    public static final String H5_STRING_IMM = "/input_file_remote";

    /**
     * Output directory to use when storing hdfs results.
     */
    public static final String H5_STRING_OUTPUT = "/output_file_remote";

    /**
     * Dynamic map file.
     */
    public static final String H5_2D_DQMAP = "/dqmap";
    
    /**
     * Static map file 
     */
    public static final String H5_2D_SQMAP = "/sqmap";

    /** 
     * Frame window size 
     **/
    public static final String H5_INT_SWSIZE = "/static_mean_window_size";

    /**
     *  Frame window size used during g2 calculation. This option is currently
     *  unused.
     */
    public static final String H5_INT_DWSIZE = "/dynamic_mean_window_size";

    /**
     * Delays per level.
     */
    public static final String H5_INT_DPL = "/delays_per_level";

    /**
     * Frame number where to start processing image.
     */
    public static final String H5_INT_FSTART_TODO = "/data_begin_todo";
    
    /**
     * Frame number upto where the usable image data resides.
     */
    public static final String H5_INT_FEND_TODO = "/data_end_todo";

    /**
     * Frame number where to start processing image.
     */
    public static final String H5_INT_FSTART = "/data_begin";
    
    /**
     * Frame number upto where the usable image data resides.
     */
    public static final String H5_INT_FEND = "/data_end";


    /**
     * Sparse mode enabled or nod, enabled=true, disabled=false.
     */
    public static final String H5_STRING_SPARSE = "/compression";

    /**
     * Frame number from which to start reading the dark image data.
     */
    public static final String H5_INT_DSTART = "/dark_begin_todo";
    
    /**
     * Frame number from which to start reading the dark image data.
     */
    public static final String H5_INT_DEND = "/dark_end_todo";

    /**
     * Thresholding value used during dark subtraction.
     */
    public static final String H5_FLOAT_THRESHOLD = "/lld";
    
    /**
     * Standard deviations to be used during dark subtraction.
     */
    public static final String H5_FLOAT_SIGMA = "/sigma";

    /**
     *  Time elapsed between slices in seconds (Kinetics Mode). 
     */
    public static final String H5_FLOAT_TIMEELAPSED = 
            "/measurement/instrument/detector/exposure_time";

    /**
     * Y coordinate of pixel at the top of the highest slice (Kinetics Mode).
     */
    public static final String H5_INT_SLICE_Y = 
            "/measurement/instrument/detector/kinetics/top";

    /**
     *  Slice height in pixels. (Kinetics Mode).
     */
    public static final String H5_INT_SLICE_HEIGHT = 
            "/measurement/instrument/detector/kinetics/window_size";

    /**
     * Starting slice number. 
     */
    public static final String H5_INT_SLICE_BEGIN = 
            "/measurement/instrument/detector/kinetics/first_usable_window";

    /**
     * Last slice number
     */
    public static final String H5_INT_SLICE_END = 
            "/measurement/instrument/detector/kinetics/last_usable_window";

    /**
     * Enable or disable Kinetics mode. 
     */
    public static final String H5_STRING_KINETICS = "/kinetics";

    /**
     * Name of camera used for recording image. 
     */
    public static final String H5_STRING_DETECTOR = 
            "/measurement/instrument/detector/manufacturer";

    /**
     * Detector parameter for efficiency
     */
    public static final String H5_FLOAT_EFFICIENCY = 
            "/measurement/instrument/detector/efficiency";
    
    /**
     * Detector parameter of adhupphot
     */
    public static final String H5_FLOAT_ADHUPPHOT = 
            "/measurement/instrument/detector/adu_per_photon";
    
    /**
     * Detector parameter of preset
     */
    public static final String H5_FLOAT_PRESET = 
            "/measurement/instrument/detector/exposure_time";
    
    /**
     * Detector parameter  X pixel size
     */
    public static final String H5_FLOAT_DPIXX = 
            "/measurement/instrument/detector/x_pixel_size";
    
    /**
     * Detector parameter Y pixel size
     */
    public static final String H5_FLOAT_DPIXY = 
            "/measurement/instrument/detector/y_pixel_size";
    
    /**
     * Detector parameter for distance
     */
    public static final String H5_FLOAT_DISTANCE = 
            "/measurement/instrument/detector/distance";
    
    /**
     * Detector parameter for flux incident
     */
    public static final String H5_FLOAT_FLUXINCIDENT = 
            "/measurement/instrument/source_begin/beam_intensity_incident";
    
    /**
     * Detector parameter for flux transmitted
     */
    public static final String H5_FLOAT_FLUXTRANSMITTED = 
            "/measurement/instrument/source_begin/beam_intensity_transmitted";
    
    /**
     * Detector parameter of sample thickness
     */
    public static final String H5_FLOAT_THICKNESS = "/measurement/sample/thickness";
    
    /**
     * Detector parameter of ring current
     */
    public static final String H5_FLOAT_RINGCURRENT = 
            "/measurement/instrument/source_begin/current";
    
    /**
     * Detector X-dimension or frame width
     */
    public static final String H5_INT_XDIM = "/measurement/instrument/detector/x_dimension";
    
    /**
     * Detector Y-dimension or frame height
     */
    public static final String H5_INT_YDIM = "/measurement/instrument/detector/y_dimension";

    /**
     * HDF5 Tag to use when storing output data
     */
    public static final String H5_STRING_OUTPUT_TAG = "/output_data";

    /**
     * X-Bin value
     */
    public static final String H5_FLOAT_XBIN = "/swbinX";

    /**
     * Y-Bin value
     */
    public static final String H5_FLOAT_YBIN = "/swbinY";
    
    public static final String H5_STRING_FLATFIELD = "/flatfield_enabled";
    
    public static final String H5_2D_FLATFIELD = "/measurement/instrument/detector/flatfield";
    
    public static final String H5_2D_FRAMESUM = "/frameSum";

    public static final String H5_2D_PIXELSUM = "/pixelSum";

    public static final String H5_2D_SG = "/SG_smoothed_data";

    public static final String H5_1D_BINTOPROCESS = "/qphi_bin_to_process";

    public static final String H5_STRING_ANALYSIS = "/analysis_type";

    public static final String H5_INT_PIXEL_FILTER_SUM = "/avg_frames";

    public static final String H5_INT_PIXEL_FILTER_STRIDE =  "/stride_frames";
}
