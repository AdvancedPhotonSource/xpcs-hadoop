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
import gov.anl.aps.xpcs.mapred.io.NormResult;
import gov.anl.aps.xpcs.mapred.io.PartitionKey;
import gov.anl.aps.xpcs.mapred.io.PartitionValue;
import gov.anl.aps.xpcs.util.QMaps;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * A reducer that calculates the normalized g2 value for each dynamic partition.
 */

public class PartitionReducer extends MapReduceBase implements
        Reducer<PartitionKey, PartitionValue, PartitionKey, NormResult> {
    // Average of iFuture values for each static bin 
    private double[] iFuturesAvg = null; 
    // Average of iPast values for each static bin. 
    private double[] iPastsAvg = null;
    // G2s contains sum of G2 values for pixels within same static partition.
    private double[] G2s = null;
    // G2sall contains G2 values for all the pixel in current partition.
    private double[] G2sall = null;
    // G2sSbin contains static bin number for each G2 value in G2sall
    private int[] G2sSbin = null;
    // Used to keep track of next G2 value for a given static partition. 
    private int[] partitionIndex = null;
    // Keeps track of static partitions of current dynamic bin. 
    private int[] partitions = null;
    // Use to output norm results.
    private NormResult normResult = new NormResult();

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        XPCSConfig config = new XPCSConfig(job);
        
        try {
            populateCounts(config);
        } catch (IOException io) {
            throw new RuntimeException("Failed to configure job "
                    + io.getMessage());
        }
    }

    /**
     * @throws IOException
     * 
     */
    private void populateCounts(XPCSConfig configuration) throws IOException {
        QMaps qmaps = new QMaps(configuration); 
        
        qmaps.setMaxPixelCountsDynamic(
                configuration.getMaxPixelCountsDynamic());

        // Total number of possible static partitions in any dynamnic bin.
        int totalStaticCount = qmaps.getTotalStaticPartitions();
        // Max number of pixels in any dynamic bin. The actual number of pixels
        //  in a given bin can be lower than this number. 
        int maxPixelCount = qmaps.getMaxPixelCountsDynamic();
        // The iF, IP and G2 arrays are bigger than what we will end up
        // using for each of the partitions Making each array's size equal
        //  to the size of total static partition makes it convenient to sum the
        //  IF, IP value for each static partition without requiring to translate
        //   indices. 
        iFuturesAvg = new double[totalStaticCount];
        iPastsAvg = new double[totalStaticCount];
        G2s = new double[totalStaticCount];
        partitionIndex = new int[totalStaticCount];
        G2sall = new double[maxPixelCount];
        G2sSbin = new int[maxPixelCount];
        partitions = new int[totalStaticCount];
    }

    private void clear() {
        Arrays.fill(iFuturesAvg, 0.0);
        Arrays.fill(iPastsAvg, 0.0);
        Arrays.fill(G2s, 0.0);
        Arrays.fill(partitionIndex, 0);
        Arrays.fill(G2sall, 0.0);
        Arrays.fill(G2sSbin, 0);
        Arrays.fill(partitions, 0);
    }

    @Override
    public void reduce(PartitionKey key, Iterator<PartitionValue> values,
            OutputCollector<PartitionKey, NormResult> output,
            Reporter reporter) throws IOException {
        clear();

        int index = 0;
        int staticPartitions = 0; //static partition count in current bin. 
        // Sum iF, iP and G2 values within each static partition.
        while (values.hasNext()) {
            PartitionValue value = values.next();
            Short s = value.getStaticPartition();
            iFuturesAvg[s] += ( value.getiFuture() - iFuturesAvg[s]) / (partitionIndex[s] + 1);
            iPastsAvg[s] += ( value.getiPast() - iPastsAvg[s]) / (partitionIndex[s] + 1);
            G2s[s] += value.getG2();
            // Save all G2 values
            G2sall[index] = value.getG2();
            G2sSbin[index] = s;
            
            // Move the partition index for given static bin
            partitionIndex[s]++;
            index++;
            
            // Only few indexes into G2s array will contain G2 values for this
            // dynamic partition. We need to keep track of them to compute G2
            // averages for same static partition.
            if (partitions[s] == 0)
                staticPartitions++;
            partitions[s] = 1;
        }

        int count = 0;
        double[] normalizedValues = new double[staticPartitions];
        for (int s=0; s<partitions.length; s++) {

            //Ignore static partitions that were not present in this bin. 
            if (partitions[s] == 0) 
                continue;

            // Filter static partitions that have there average IF or IP value zero.
            // A zero value for IFavg or IPavg will cause the g2 to become a Nan. 
            if (iFuturesAvg[s] == 0 || iPastsAvg[s] == 0) {
                partitions[s] = 0; //Setting this to zero will also filter out this sbin value 
                                   // when computing standard error. 
                continue;
            }

            double normalizedG2 = normalize(
                    G2s[s] / (double)partitionIndex[s], 
                    s);
            normalizedValues[count] = normalizedG2;
            count++;
        }

        double partitionMean = calculatePartitionMean(normalizedValues, count);
        normResult.setMean(partitionMean);
        
        double samples = 1.0;
        double tempAvg = 0.0;
        double avgG2 = 0.0;
        double stdG2 = 0.0;
        
        for (int j=0; j<index; j++) {
            // running average and standard deviation.
            tempAvg = avgG2;

            // When normalizing all G2 values for computing standard-error,
            //  exclude those G2s that lies in the static bin that were excluded
            //    in the previous step. 
            if (partitions[G2sSbin[j]] == 0 ) {
                continue;
            }

            // Normalize each G2 value for computing standard error.
            double normalizedG2 = normalize(G2sall[j], G2sSbin[j]);
            avgG2 += ((normalizedG2 - tempAvg) / samples);
            stdG2 += ((normalizedG2 - tempAvg) * (normalizedG2 - avgG2));
            samples = samples + 1.0;
        }
        
        // sqrt(1/#of pixels in this damp) * sqrt(standard_deviation of all G2s in dmap);
        double stdError = Math.sqrt( 1 / (samples - 1) ) * Math.sqrt(stdG2 / (samples -1));
        normResult.setError(stdError);
        
        output.collect(key, normResult);
    }

    /**
     * @param normalizedValues
     * @return
     */
    private double calculatePartitionMean(double[] normalizedValues, int count) {
        double mean = 0.0d;
        double sum = 0.0d;
        
        for (int i=0; i<count; i++) {
            sum += normalizedValues[i];
        }
        mean = sum / (double) normalizedValues.length;
        
        return mean;
    }

    private double normalize(double G2, int staticPartition) {
        return G2 / (iFuturesAvg[staticPartition] * iPastsAvg[staticPartition]);
    }

}
