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

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import gov.anl.aps.xpcs.mapred.Header;
import gov.anl.aps.xpcs.config.XPCSConfig;
import gov.anl.aps.xpcs.config.XPCSHDFConfig;

public class BinFlatFieldIMMFile {
	private static byte[] buffer = new byte[8 * 1024 * 1024];

	private static byte[] MAGIC_MARKER = { (byte) 0xFF, (byte) 0xFF,
			(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
			(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };

	protected static final int BYTES_PER_PIXEL_VALUE = 2;

	protected static final int BYTES_PER_PIXEL_INDEX = 4;

	protected static final int BYTES_PER_PIXEL = BYTES_PER_PIXEL_VALUE
			+ BYTES_PER_PIXEL_INDEX;

	private static String inputFile = "";
	private static boolean verbose = false;
	private static Header header = new Header();
	private static boolean compression = false;
	
	private static XPCSConfig configuration = null;

	private static String hdf5File = "";
	
	private static void convert() throws Exception {

		configuration = new XPCSConfig(new Configuration());
		configuration.setHDF5ConfigFile(hdf5File);

		XPCSHDFConfig hdf5Config = new XPCSHDFConfig(configuration);
		
		Path inputPath = new Path(inputFile);
		FileSystem hdfs = inputPath.getFileSystem(new Configuration());
		FSDataInputStream fin = hdfs.open(inputPath);

		long pos = 0;
		int frames = 0;
		int framelen = 0;

		while (fin.available() > 0) {
			try {
				fin.readFully(buffer, 0, Header.HEADER_SIZE_IN_BYTES);
				header.update(buffer);

				// TODO Format the output.
				// Compute frame length based on the compression.
				if (compression) {
					framelen = header.getPixelCount()
							* XPCSConfig.BYTES_PER_PIXEL;
				} else {
					framelen = header.getPixelCount()
							* XPCSConfig.BYTES_PER_PIXEL_VALUE;
				}

				fin.readFully(buffer, 0,
						BYTES_PER_PIXEL * header.getPixelCount());

				IntBuffer indexBuffer = ByteBuffer
						.wrap(buffer, 0,
								BYTES_PER_PIXEL_INDEX * header.getPixelCount())
						.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
				ShortBuffer valueBuffer = ByteBuffer
						.wrap(buffer,
								BYTES_PER_PIXEL_INDEX * header.getPixelCount(),
								BYTES_PER_PIXEL_VALUE * header.getPixelCount())
						.order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();

				int [] image = toImage(indexBuffer, valueBuffer);
				
				frames++;
			} catch (IOException io) {
				io.printStackTrace();
				break;
			}
		}

		fin.close();
	}

	 private static int[] toImage(IntBuffer index, ShortBuffer value) {
		 int width = configuration.getFrameWidth();
		 int height = configuration.getFrameHeight();
		 
		 int image[] = new int[width * height];
		 
		 try {
			 while (true) {
				 int i = index.get();
				 int v = value.get();
				 
				 image[i] = v;
			 }
		 } catch (BufferUnderflowException buuf){}
		 
		 
		 return image;
	 }

	/**
	 * Check the command line for required parameters and display a help message
	 * if something is wrong.
	 * 
	 * @param args
	 *            - command line parameters from main()
	 * @return true if everything is okay, false otherwise
	 */
	public static boolean parseCommandLine(String[] args) {

		// Create the command line parser
		CommandLineParser parser = new PosixParser();

		// Help formatter
		HelpFormatter formatter = new HelpFormatter();

		// Create the Options
		Options options = new Options();
		options.addOption("i", "path", true, "Path to IMM file (Required)");
		options.addOption("h", "h5 file", true, "Path to HDF5 file (Required)");
		options.addOption("s", "sparse", false,
				"IMM file is sparse (default:no, Optional)");
		
		// Parse command line options
		try {

			// Parse command line
			CommandLine line = parser.parse(options, args);

			// Check for input path
			if (line.hasOption("i")) {
				inputFile = line.getOptionValue("i");
			} else {
				System.err.println("Missing option: i");
				formatter.printHelp("BinFlatFieldIMMFile", options);
				return false;
			}
			
			if (line.hasOption("h")) {
				hdf5File = line.getOptionValue("h");
			} else {
				System.err.println("Missing option h");
				formatter.printHelp("BinFlatFieldIMMFile", options);
			}

			// Check for sparse mode file
			if (line.hasOption("s")) {
				compression = true;

			}
		}

		// Catch exceptions for bad command line arguments, and
		// missing parameters
		catch (ParseException exp) {
			System.out.println(exp.getMessage());
			formatter.printHelp("LaunchJob", options);
			return false;
		}

		// Command line ok
		return true;

	}

	public static void main(String args[]) throws IOException {

		// Parse command line
		if (parseCommandLine(args) == false) {
			return;
		}

		try {
			BinFlatFieldIMMFile.convert();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
