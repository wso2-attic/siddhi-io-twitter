package org.wso2.extension.siddhi.io.twitter.util;

import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

/**
 * This extracts the parameters that need to be extracted in to parts.
 */

public class ExtractParam {
    private static int length;
    private static int i;

        public static String[] extract(String str) {
            return str.split(",");
        }

        public static double[][] locationParam (String locationParam) {
            String[] locationPair;
            double[][] locations;
            length = extract(locationParam).length;
            locationPair = new String[length];
            for (i = 0; i < length; ++i) {
                locationPair[i] = extract(locationParam)[i];
            }
            length = locationPair.length;
            locations = new double[length][2];
            for (i = 0; i < length; ++i) {
                try {
                    locations[i][0] = Double.parseDouble(locationPair[i].split(":")[0]);
                    locations[i][1] = Double.parseDouble(locationPair[i].split(":")[1]);
                } catch (NumberFormatException e) {
                    throw new SiddhiAppValidationException("Latitude/Longitude should be a double value : " + e);
                }
            }
            return locations;
        }

        public static long[] followParam(String followParam) {
            long[] follow;
            length = followParam.split(",").length;
            follow = new long[length];
            for (i = 0; i < length; i++) {
                try {
                    follow[i] = Long.parseLong(followParam.split(",")[i]);
                } catch (NumberFormatException e) {
                    throw new SiddhiAppValidationException("Follow should be a long value: " + e);
                }
            }
            return follow;
        }
    }

