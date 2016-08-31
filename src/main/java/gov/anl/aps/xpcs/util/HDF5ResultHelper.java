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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import javax.swing.tree.DefaultMutableTreeNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.Datatype;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.h5.H5File;


//TODO: Refactor this class. 
// (i) Make the name of the output dataset as static constant
// (ii) Ability to handle generic output .


public class HDF5ResultHelper {

	private static Logger logger = LoggerFactory.getLogger(HDF5ResultHelper.class);

	private static final String FRAME_SUM_NAME = "frameData";
	private static final String PIXEL_SUM_NAME = "pixelSums";
	private static final String PARTITION_MEAN_NAME = "partition-mean";
	private static final String DARK_AVG_NAME = "darkAverages.txt";
	private static final String DARK_STD_NAME = "darkStdDev.txt";
	private static final String TWOTIME = "TwoTimes";
	private static final String RESULTS_NAME = "norm/results";

	public static void writeMultiTau(XPCSConfig configuration, String path) throws Exception {

		String hdf5 = configuration.getHDF5ConfigFile();
		File filePath = checkValidPath(path);
		File frameSum = checkFileExistence(path, HDF5ResultHelper.FRAME_SUM_NAME);
		File pixelSum = checkFileExistence(path, HDF5ResultHelper.PIXEL_SUM_NAME);
		File partitionMean = checkFileExistence(path, HDF5ResultHelper.PARTITION_MEAN_NAME);

		double meanFactor = PartitionMeanNormFactor.computeNormFactor(configuration);

		File results = checkFileExistence(path, HDF5ResultHelper.RESULTS_NAME);

		File destination = new File(hdf5);

		H5File hFile = openHDF5File(destination);

		Group root = (Group) ((DefaultMutableTreeNode) hFile.getRootNode()).getUserObject();
		if (root == null)
			throw new RuntimeException("root null");

		Group hadoopGroup = (Group) hFile.get(configuration.getOutputTag());
		if (hadoopGroup == null)
			hadoopGroup = hFile.createGroup(configuration.getOutputTag(), root);

		logger.info("Writing FrameSum...");
		writeFrameSumTimeStamps(configuration,
							    "frameSum", 
							    "timestamp_clock", 
							    "timestamp_tick",
							    frameSum, // actual dataset
							    hFile,  // the hdf5 file object
							    hadoopGroup // the dataset name. 
							   );

		logger.debug("Writing PixelSum...");
		// writePixelSum1(pixelSum, hFile, hadoopGroup);
		writePixelSum(configuration, "pixelSum", pixelSum, hFile, hadoopGroup);

		logger.debug("Writing Tau...");
		writeTau("tau", results, hFile, hadoopGroup);

		if (configuration.getIsKinetics()) {
			writeTauKinetics("tau-kinetics", results, hFile, hadoopGroup);
		}

		logger.debug("Writing Partition Mean...");
		writePartitionMean("partition-mean", partitionMean, "partition_norm_factor", meanFactor, hFile, hadoopGroup);

		logger.debug("Writing norm-0...");
		writeNormG21(configuration, "norm-0", results, hFile, hadoopGroup);
		// writeNormG22(configuration,"norm",results, hFile, hadoopGroup);

		try {
			File darkAvg = checkFileExistence(path, HDF5ResultHelper.DARK_AVG_NAME);
			logger.debug("Writing dark-averages...");
			writeMatrix("darkAverages", darkAvg, hFile, hadoopGroup);
		} catch (RuntimeException e) {
			logger.debug("Dark-averages does not exists");
		}

		try {
			File darkStd = checkFileExistence(path, HDF5ResultHelper.DARK_STD_NAME);

			logger.debug("Writing dar-std...");
			writeMatrix("darkStdDev", darkStd, hFile, hadoopGroup);
		} catch (RuntimeException e) {
			logger.debug("Dark-std does not exists");
		}

		// close file resource
		hFile.close();

	}

	public static void writeTwoTime(XPCSConfig configuration) throws Exception {

		String hdf5 = configuration.getHDF5ConfigFile();
		
		File destination = new File(hdf5);

		H5File hFile = openHDF5File(destination);

		Group root = (Group) ((DefaultMutableTreeNode) hFile.getRootNode()).getUserObject();

		if (root == null)
			throw new RuntimeException("root null");

		Group hadoopGroup = (Group) hFile.get(configuration.getOutputTag());
		if (hadoopGroup == null)
			hadoopGroup = hFile.createGroup(configuration.getOutputTag(), root);

		logger.debug("Writing two-time correlations...");
		writeNormTwoTime(configuration, hFile, hadoopGroup);

		// close file resource
		hFile.close();

	}

	// Helpers
	private static File checkValidPath(String path) {
		File filePath = new File(path);
		if (!filePath.isDirectory() || !filePath.exists()) {
			throw new RuntimeException("Path does not exist or is not a directory");
		} else
			return filePath;
	}

	private static File checkFileExistence(String path, String file) {
		File filePath = new File(path, file);

		if (!filePath.exists()) {
			throw new RuntimeException("File does not exist");
		} else if (filePath.isDirectory()) {
			throw new RuntimeException("File is a directory");
		} else
			return filePath;
	}

	private static H5File openHDF5File(File destination) throws Exception {

		H5File hFile = new H5File(destination.getAbsolutePath(), FileFormat.WRITE);
		// Open
		hFile.open();
		return hFile;
	}

	private static void writeFrameSumTimeStamps(XPCSConfig configuration, 
											    String name, 
											    String timeStampClockName,
											    String timeStampTickName,
											    File frameSum, 
											    H5File hFile, 
											    Group hadoopGroup) throws Exception {

		int frameCount = configuration.getFramecount();
		if (configuration.getIsKinetics()) {
			int slicesPerFrame = configuration.getLastSlice() - configuration.getFirstSlice() + 1;
			frameCount *= slicesPerFrame;
		}
		long[] dims2D = { 2, frameCount };
		Datatype dtype = hFile.createDatatype(Datatype.CLASS_FLOAT, 8, Datatype.NATIVE, Datatype.NATIVE);

		Dataset dataset1 = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + name);
		if (dataset1 == null) {
			dataset1 = hFile.createScalarDS(name, hadoopGroup, dtype, dims2D, null, null, 0, null);
		}

		Dataset dataset2 = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + timeStampClockName);
		if (dataset2 == null) {
			dataset2 = hFile.createScalarDS(timeStampClockName, hadoopGroup, dtype, dims2D, null, null, 0, null);
		}

		Dataset dataset3 = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + timeStampTickName);
		if (dataset3 == null) {
			dataset3 = hFile.createScalarDS(timeStampTickName, hadoopGroup, dtype, dims2D, null, null, 0, null);
		}

		BufferedReader reader = new BufferedReader(new FileReader(frameSum));

		double[] buffer1 = new double[(int) (dims2D[0] * dims2D[1])];
		double[] buffer2 = new double[(int) (dims2D[0] * dims2D[1])];
		double[] buffer3 = new double[(int) (dims2D[0] * dims2D[1])];

		String line = reader.readLine();

		
		while (line != null) {
			String[] tokens = line.split(",");

			double index = (double) Integer.parseInt(tokens[0]);
			double sum = (double) Double.parseDouble(tokens[1]);
			double clock = (double) Double.parseDouble(tokens[2]);
			double tick = (double) Double.parseDouble(tokens[3]);

			buffer1[(int) (0 * dims2D[1] + index)] = index + 1;
			buffer1[(int) (1 * dims2D[1] + index)] = sum;

			buffer2[(int) (0 * dims2D[1] + index)] = index + 1;
			buffer2[(int) (1 * dims2D[1] + index)] = clock;

			buffer3[(int) (0 * dims2D[1] + index)] = index + 1;
			buffer3[(int) (1 * dims2D[1] + index)] = tick;

			line = reader.readLine();
		}

		reader.close();

		write2DDataset(dataset1, 0, 0, 2, (int) dims2D[1], buffer1); // frame sum
		write2DDataset(dataset2, 0, 0, 2, (int) dims2D[1], buffer2); // frame clock (elapsed time)
		write2DDataset(dataset3, 0, 0, 2, (int) dims2D[1], buffer3); // frame tick

	}

	private static void writePixelSum(XPCSConfig configuration, String name, File pixelSum, H5File hFile,
			Group hadoopGroup) throws Exception {

		Integer h = configuration.getFrameHeight() / (int) configuration.getBinY();
		Integer w = configuration.getFrameWidth() / (int) configuration.getBinX();

		long[] dims2D = { h, w };
		Datatype dtype = hFile.createDatatype(Datatype.CLASS_FLOAT, 8, Datatype.NATIVE, Datatype.NATIVE);

		Dataset dataset = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + name);
		if (dataset == null) {
			dataset = hFile.createScalarDS(name, hadoopGroup, dtype, dims2D, null, null, 0, null);
		}

		BufferedReader reader = new BufferedReader(new FileReader(pixelSum));

		double[] buffer = new double[(int) (dims2D[0] * dims2D[1])];

		String line = reader.readLine();
		while (line != null) {
			String[] tokens = line.split(",");
			int x = Integer.parseInt(tokens[0]);
			int y = Integer.parseInt(tokens[1]);
			double sum = (double) Double.parseDouble(tokens[2]);

			buffer[(int) (y * dims2D[1] + x)] = sum;

			line = reader.readLine();
		}

		write2DDataset(dataset, 0, 0, (int) dims2D[0], (int) dims2D[1], buffer);

		reader.close();
	}

	private static void writeTau(String name, File results, H5File hFile, Group hadoopGroup) throws Exception {
		Double[] tauSet = getPossibleDoubleValues(results, 1);
		ArrayList<Double> tauSetPositive = new ArrayList<Double>();

		for (double d : tauSet) {
			if (d >= 0)
				tauSetPositive.add(d);
		}

		tauSet = tauSetPositive.toArray(new Double[0]);
		long[] dims2D = { 1, tauSet.length };
		Datatype dtype = hFile.createDatatype(Datatype.CLASS_FLOAT, 8, Datatype.NATIVE, Datatype.NATIVE);

		Dataset dataset = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + name);
		if (dataset == null) {
			dataset = hFile.createScalarDS(name, hadoopGroup, dtype, dims2D, null, null, 0, null);
		}

		write2DDataset(dataset, 0, 0, 1, tauSet.length, tauSet);
	}

	private static void writeTauKinetics(String name, File results, H5File hFile, Group hadoopGroup) throws Exception {
		Double[] tauSet = getPossibleDoubleValues(results, 1);
		ArrayList<Double> tauSetSlices = new ArrayList<Double>();

		for (double d : tauSet) {
			if (d < 0)
				tauSetSlices.add(d);
		}

		tauSet = tauSetSlices.toArray(new Double[0]);

		for (int i = 0; i < tauSet.length; i++) {
			tauSet[i] = -1.0 * tauSet[i];
		}

		Arrays.sort(tauSet);

		long[] dims2D = { 1, tauSet.length };
		Datatype dtype = hFile.createDatatype(Datatype.CLASS_FLOAT, 8, Datatype.NATIVE, Datatype.NATIVE);

		Dataset dataset = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + name);
		if (dataset == null) {
			dataset = hFile.createScalarDS(name, hadoopGroup, dtype, dims2D, null, null, 0, null);
		}

		write2DDataset(dataset, 0, 0, 1, tauSet.length, tauSet);
	}

	private static void writePartitionMean(String name, 
										   File partitionMean, 
										   String normFactorName,
										   double normFactor,
										   H5File hFile, 
										   Group hadoopGroup)
			throws Exception {

		Integer[] a = getPossibleIntegerValues(partitionMean, 0);
		Integer[] b = getPossibleIntegerValues(partitionMean, 1);
		Integer[] c = getPossibleIntegerValues(partitionMean, 2);

		int minB = b[0];
		int maxC = c[c.length - 1];

		HashMap<Integer, Integer> indexMap = new HashMap<Integer, Integer>();
		for (int i = 0; i < b.length; i++) {
			indexMap.put(b[i], i);
		}

		long[] dims2D1 = { b.length, a.length };
		long[] dims2D2 = { 1, a.length };
		double[] buffer1 = new double[(int) (dims2D1[0] * dims2D1[1])];
		double[] buffer2 = new double[(int) (dims2D2[0] * dims2D2[1])];
		Datatype dtype = hFile.createDatatype(Datatype.CLASS_FLOAT, 8, Datatype.NATIVE, Datatype.NATIVE);

		Dataset dataset1 = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + name + "-partial");
		if (dataset1 == null) {
			dataset1 = hFile.createScalarDS(name + "-partial", hadoopGroup, dtype, dims2D1, null, null, 0, null);
		}

		Dataset dataset2 = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + name + "-total");
		if (dataset2 == null) {
			dataset2 = hFile.createScalarDS(name + "-total", hadoopGroup, dtype, dims2D2, null, null, 0, null);
		}

		long[] dims1D = {1, 1};

		Dataset dataset3 = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + normFactorName);
		if (dataset3 == null) {
			dataset3 = hFile.createScalarDS(normFactorName, hadoopGroup, dtype, dims1D, null, null, 0, null);
		}		
		dataset3.write(new double[]{normFactor});
		

		BufferedReader reader = new BufferedReader(new FileReader(partitionMean));
		String line = reader.readLine();
		while (line != null) {
			String[] tokens = line.split(",");

			int col = Integer.parseInt(tokens[0]) - 1;
			int rowS = Integer.parseInt(tokens[1]);
			int rowE = Integer.parseInt(tokens[2]);
			double value = Double.parseDouble(tokens[3]);

			if (!tokens[3].equals("NaN")) {
				if ((rowS != minB || rowE != maxC)) {
					buffer1[(int) (indexMap.get(rowS) * dims2D1[1] + col)] = value;
				} else {
					buffer2[(int) (0 * dims2D2[1] + col)] = value;
				}
			}

			line = reader.readLine();
		}


		write2DDataset(dataset1, 0, 0, (int) dims2D1[0], (int) dims2D1[1], buffer1);
		write2DDataset(dataset2, 0, 0, (int) dims2D2[0], (int) dims2D2[1], buffer2);

		reader.close();
	}

	private static void writeNormTwoTime(XPCSConfig configuration, 
                                    H5File hFile, 
                                    Group hadoopGroup) throws Exception {
		
        int frameCount = configuration.getFramecount();

        Group twoTimeGroup = (Group) hFile.get(hadoopGroup.getFullName() + "/TwoTime");

        if(twoTimeGroup == null)
         twoTimeGroup = hFile.createGroup(hadoopGroup.getFullName() + "/TwoTime", hadoopGroup);

		long[] dims3D = {frameCount, frameCount, 1};
		long[] chunks = {frameCount, frameCount, 1};
		Datatype dtype = hFile.createDatatype(
		        Datatype.CLASS_FLOAT, 8, Datatype.NATIVE, Datatype.NATIVE);

	  	Dataset dataset1 = null;

	  	// Number of bytes would be at-least half of total expected data i.e sizeOfDouble * frameCount * frameCount
		byte[] buffer1 = new byte[(4 * frameCount * frameCount)];
		byte[] buffer2 = new byte[8];

		long bytesRead = 0;

		Path inputPath = new Path(configuration.getOutputDir() + "/g2");
		FileSystem hdfs = FileSystem.get(configuration);
		FileStatus[] fls = hdfs.listStatus(inputPath);

		QMaps partitions = configuration.getMaps();
        int pixelsPerBin[] = partitions.getDynamicCounts();

		for (FileStatus fl : fls) {
			if (fl.getPath().getName().contains("part-"))
			{
				BufferedInputStream buffin = new BufferedInputStream(hdfs.open(fl.getPath()));
				bytesRead = buffin.read(buffer2, 0, buffer2.length);

				ByteBuffer q = ByteBuffer.wrap(buffer2, 0, (int)bytesRead);
				int qvalue = (int) q.getDouble();
				bytesRead = buffin.read(buffer1, 0, buffer1.length);

				DoubleBuffer data = ByteBuffer.wrap(buffer1, 0, (int)bytesRead).asDoubleBuffer();

				double out[][] = new double[frameCount][frameCount];

				int index = 1, prevI = 0,  i = 0, j = 0, els = 0;

				int totalEls = (int) (Math.pow(frameCount-1, 2) + (frameCount - 1)) / 2;

				while (els <  totalEls)
				{	

					i = index / frameCount;
					j = index % frameCount;

					if (prevI != i)
					{
						j += (i + 1);
						index += (i + 1);
					}
					
					out[i][j] = data.get() / pixelsPerBin[qvalue];

					index++;
					els++;
					prevI = i;
				}


				String name = twoTimeGroup.getFullName() + "/" + getFormattedQName(qvalue, 4);
		    	dataset1 = (Dataset) hFile.get(name);
		    	if(dataset1 == null) {
			        dataset1 = hFile.createScalarDS(name, twoTimeGroup, dtype, dims3D, null, chunks, 6, null);
		    	}        

				write3DDataset(dataset1,
					           0,
					           0,
					           0,
					           (int) dims3D[0],
					           (int) dims3D[1], 
					           (int) dims3D[2], 
					           out);
			}
		}
	}

	private static String getFormattedQName(int q, int zeros)
	{
		String result = "C_";
		zeros -=  q / 10;

		for (int i =0; i < zeros - 1; i++) {
			result += "0";
		}

		result += q;

		return result;
	}

	private static void writeNormG21(XPCSConfig configuration, String name, File results, H5File hFile,
			Group hadoopGroup) throws Exception {

		Double[] tauSet = getPossibleDoubleValues(results, 1);

		// Among tauSet count how many tau values belong to kinetics mode.
		// This count is equal to number of negative tau values in the set.
		int tauKineticsCount = 0;

		ArrayList<Double> tauSetFrames = new ArrayList<Double>();
		for (double d : tauSet) {
			if (d >= 0)
				tauSetFrames.add(d);
			else
				tauKineticsCount++;
		}

		tauSet = tauSetFrames.toArray(new Double[0]);

		Integer[] dynIDSet = getPossibleIntegerValues(results, 0);

		int frameCount = configuration.getFramecount();
		if (configuration.getIsKinetics()) {
			int slicesPerFrame = configuration.getLastSlice() - configuration.getFirstSlice() + 1;
			frameCount *= slicesPerFrame;
		}

		// TODO - Right now it is fixed and not in the txt file
		Integer[] numFramesSet = new Integer[1];
		numFramesSet[0] = frameCount;

		HashMap<Double, Integer> tauTable = new HashMap<Double, Integer>();
		HashMap<Integer, Integer> dynTable = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> numFramesTable = new HashMap<Integer, Integer>();

		for (int i = 0; i < tauSet.length; i++) {
			tauTable.put(tauSet[i], i);
		}
		for (int i = 0; i < dynIDSet.length; i++) {
			dynTable.put(dynIDSet[i], i);
		}
		for (int i = 0; i < numFramesSet.length; i++) {
			numFramesTable.put(numFramesSet[i], i);
		}

		long[] dims3D = { tauSet.length + tauKineticsCount, dynIDSet.length, numFramesSet.length };
		Datatype dtype = hFile.createDatatype(Datatype.CLASS_FLOAT, 8, Datatype.NATIVE, Datatype.NATIVE);

		Dataset dataset1 = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + name + "-g2");
		if (dataset1 == null) {
			dataset1 = hFile.createScalarDS(name + "-g2", hadoopGroup, dtype, dims3D, null, null, 0, null);
		}
		Dataset dataset2 = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + name + "-stderr");
		if (dataset2 == null) {
			dataset2 = hFile.createScalarDS(name + "-stderr", hadoopGroup, dtype, dims3D, null, null, 0, null);
		}

		double[] buffer1 = new double[(int) (dims3D[0] * dims3D[1] * dims3D[2])];
		double[] buffer2 = new double[(int) (dims3D[0] * dims3D[1] * dims3D[2])];

		BufferedReader reader = new BufferedReader(new FileReader(results));

		String line = reader.readLine();
		while (line != null) {
			String[] tokens = line.split(",");
			double tau = (double) Double.parseDouble(tokens[1]);
			int dynID = Integer.parseInt(tokens[0]);
			double avgG2 = (double) Double.parseDouble(tokens[2]);
			// int numFrame = Integer.parseInt(tokens[2]);
			double sterr = Double.parseDouble(tokens[3]);
			int x;
			if (tau < 0) {
				x = (int) (-1.0 * tau) - 1;
			} else {
				x = tauKineticsCount + tauTable.get(tau);
			}

			int y = dynTable.get(dynID);
			// int z = numFramesTable.get(numFrame);
			int z = numFramesTable.get(frameCount);

			buffer1[(int) (z * dims3D[2] * dims3D[1] + x * dims3D[1] + y)] = avgG2;
			buffer2[(int) (z * dims3D[2] * dims3D[1] + x * dims3D[1] + y)] = sterr;

			line = reader.readLine();
		}

		write3DDataset(dataset1, 0, 0, 0, (int) dims3D[0], (int) dims3D[1], (int) dims3D[2], buffer1);
		write3DDataset(dataset2, 0, 0, 0, (int) dims3D[0], (int) dims3D[1], (int) dims3D[2], buffer2);

		reader.close();

	}

	private static void writeNormG22(XPCSConfig configuration, String name, File results, H5File hFile,
			Group hadoopGroup) throws Exception {

		Double[] tauSet = getPossibleDoubleValues(results, 1);

		// Among tauSet count how many tau values belong to kinetics mode.
		// This count is equal to number of negative tau values in the set.
		int tauKineticsCount = 0;

		ArrayList<Double> tauSetFrames = new ArrayList<Double>();
		for (double d : tauSet) {
			if (d >= 0)
				tauSetFrames.add(d);
			else
				tauKineticsCount++;
		}

		tauSet = tauSetFrames.toArray(new Double[0]);

		Integer[] dynIDSet = getPossibleIntegerValues(results, 0);

		int frameCount = configuration.getFramecount();
		if (configuration.getIsKinetics()) {
			int slicesPerFrame = configuration.getLastSlice() - configuration.getFirstSlice() + 1;
			frameCount *= slicesPerFrame;
		}

		// TODO - Right now it is fixed and not in the txt file
		Integer[] numFramesSet = new Integer[1];
		numFramesSet[0] = frameCount;

		HashMap<Double, Integer> tauTable = new HashMap<Double, Integer>();
		HashMap<Integer, Integer> dynTable = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> numFramesTable = new HashMap<Integer, Integer>();

		for (int i = 0; i < tauSet.length; i++) {
			tauTable.put(tauSet[i], i);
		}
		for (int i = 0; i < dynIDSet.length; i++) {
			dynTable.put(dynIDSet[i], i);
		}
		for (int i = 0; i < numFramesSet.length; i++) {
			numFramesTable.put(numFramesSet[i], i);
		}

		long[] dims3D = { tauSet.length + tauKineticsCount, dynIDSet.length, numFramesSet.length };

		int[] memberSize = { 1, 1 };
		String[] memberNames = { "g2", "g2StdErr" };

		Datatype[] memberDatatypes = { hFile.createDatatype(Datatype.CLASS_FLOAT, 8, Datatype.NATIVE, Datatype.NATIVE),
				hFile.createDatatype(Datatype.CLASS_FLOAT, 8, Datatype.NATIVE, Datatype.NATIVE) };

		Dataset dataset = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + name);
		if (dataset == null) {
			dataset = hFile.createCompoundDS(name, hadoopGroup, dims3D, null, null, 0, memberNames, memberDatatypes,
					memberSize, null);
		}

		Double[] buffer1 = new Double[(int) (dims3D[0] * dims3D[1] * dims3D[2])];
		Double[] buffer2 = new Double[(int) (dims3D[0] * dims3D[1] * dims3D[2])];

		BufferedReader reader = new BufferedReader(new FileReader(results));

		String line = reader.readLine();
		while (line != null) {
			String[] tokens = line.split(",");
			double tau = (double) Double.parseDouble(tokens[1]);
			int dynID = Integer.parseInt(tokens[0]);
			double avgG2 = (double) Double.parseDouble(tokens[2]);
			// int numFrame = Integer.parseInt(tokens[2]);
			double sterr = Double.parseDouble(tokens[3]);
			// double avgG2 = (double) Double.parseDouble(tokens[3]);
			// int numFrame = Integer.parseInt(tokens[2]);
			// double sterr = Double.parseDouble(tokens[4]);

			int x;
			if (tau < 0) {
				x = (int) (-1.0 * tau) - 1;
			} else {
				x = tauKineticsCount + tauTable.get(tau);
			}

			int y = dynTable.get(dynID);
			// int z = numFramesTable.get(numFrame);
			int z = numFramesTable.get(frameCount);
			buffer1[(int) (z * (dims3D[0] * dims3D[1]) + x * (dims3D[1]) + y)] = avgG2;
			buffer2[(int) (z * (dims3D[0] * dims3D[1]) + x * (dims3D[1]) + y)] = sterr;

			line = reader.readLine();
		}

		Vector<Double[]> v = new Vector<Double[]>();
		v.add(buffer1);
		v.add(buffer2);

		write3DDataset(dataset, 0, 0, 0, (int) dims3D[0], (int) dims3D[1], (int) dims3D[2], v);

		reader.close();
	}

	private static void writeMatrix(String name, File darkAvg, H5File hFile, Group hadoopGroup) throws Exception {
		int[] size = getSize(darkAvg);
		int rows = size[0];
		int cols = size[1];

		long[] dims2D = { rows, cols };
		Datatype dtype = hFile.createDatatype(Datatype.CLASS_FLOAT, 8, Datatype.NATIVE, Datatype.NATIVE);

		Dataset dataset = (Dataset) hFile.get(hadoopGroup.getFullName() + "/" + name);
		if (dataset == null) {
			dataset = hFile.createScalarDS(name, hadoopGroup, dtype, dims2D, null, null, 0, null);
		}

		BufferedReader reader = new BufferedReader(new FileReader(darkAvg));
		double[] buffer = new double[(int) (dims2D[0] * dims2D[1])];

		String line = reader.readLine();
		int rowIndex = 0;
		while (line != null) {
			String[] tokens = line.split(",");

			for (int colIndex = 0; colIndex < tokens.length; colIndex++) {
				double val = Double.parseDouble(tokens[colIndex]);
				buffer[(int) (rowIndex * dims2D[1] + colIndex)] = val;
			}

			rowIndex++;
			line = reader.readLine();
		}

		write2DDataset(dataset, 0, 0, (int) dims2D[0], (int) dims2D[1], buffer);
		reader.close();
	}

	private static void write3DDataset(Dataset dataset, int x, int y, int z, int h, int w, int d, Object data)
			throws Exception {

		dataset.init();

		int rank = dataset.getRank();
		long[] start = dataset.getStartDims();
		long[] stride = dataset.getStride();
		long[] sizes = dataset.getSelectedDims();

		start[0] = x;
		start[1] = y;
		start[2] = z;

		stride[0] = 1;
		stride[1] = 1;
		stride[2] = 1;

		sizes[0] = h;
		sizes[1] = w;
		sizes[2] = d;

		dataset.write(data);
	}

	private static void write2DDataset(Dataset dataset, int x, int y, int h, int w, Object data) throws Exception {

		dataset.init();

		int rank = dataset.getRank();
		long[] start = dataset.getStartDims();
		long[] stride = dataset.getStride();
		long[] sizes = dataset.getSelectedDims();

		start[0] = x;
		start[1] = y;

		stride[0] = 1;
		stride[1] = 1;

		sizes[0] = h;
		sizes[1] = w;

		dataset.write(data);
	}

	private static void write1DDataset(Dataset dataset, int x, int w, Object data) throws Exception {

		dataset.init();

		int rank = dataset.getRank();
		long[] start = dataset.getStartDims();
		long[] stride = dataset.getStride();
		long[] sizes = dataset.getSelectedDims();

		start[0] = x;
		stride[0] = 1;
		sizes[0] = w;

		dataset.write(data);
	}

	private static String readString(Dataset dataset) throws OutOfMemoryError, Exception {
		dataset.init();

		int rank = dataset.getRank();
		long[] start = dataset.getStartDims();
		long[] stride = dataset.getStride();
		long[] sizes = dataset.getSelectedDims();

		start[0] = 0;
		stride[0] = 1;
		sizes[0] = 1;

		String[] array = (String[]) dataset.read();
		return array[0];
	}

	private static Integer readInteger(Dataset dataset) throws OutOfMemoryError, Exception {
		dataset.init();

		int[] array = (int[]) dataset.read();

		return array[0];

	}

	private static Double[] getPossibleDoubleValues(File results, int col) throws IOException {
		HashSet<Double> hashSet = new HashSet<Double>();

		BufferedReader reader = new BufferedReader(new FileReader(results));
		String line = reader.readLine();
		while (line != null) {

			String[] tokens = line.split(",");
			double tau = (double) Double.parseDouble(tokens[col]);

			hashSet.add(tau);

			line = reader.readLine();
		}

		Double[] result = new Double[hashSet.size()];
		hashSet.toArray(result);

		Arrays.sort(result);

		reader.close();

		return result;
	}

	private static Integer[] getPossibleIntegerValues(File results, int col) throws IOException {
		HashSet<Integer> hashSet = new HashSet<Integer>();

		BufferedReader reader = new BufferedReader(new FileReader(results));
		String line = reader.readLine();
		while (line != null) {
			String[] tokens = line.split(",");
			int tau = (int) Integer.parseInt(tokens[col]);

			hashSet.add(tau);

			line = reader.readLine();
		}

		Integer[] result = new Integer[hashSet.size()];
		hashSet.toArray(result);

		Arrays.sort(result);

		reader.close();

		return result;
	}

	private static int[] getSize(File file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = reader.readLine();
		int cols = -1;
		int lineCount = 0;
		while (line != null) {
			lineCount++;
			String[] tokens = line.split(",");
			int numCols = tokens.length;
			if (cols == -1)
				cols = numCols;
			else if (cols != numCols) {
				throw new RuntimeException("Not properly formatted file");
			}

			line = reader.readLine();
		}

		int[] size = new int[2];

		size[0] = lineCount;
		size[1] = cols;

		return size;
	}

}
