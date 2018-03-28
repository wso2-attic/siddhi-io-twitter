/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.io.twitter.util;

import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

/**
 * This class contains util methods for the extensions.
 */
public class Util {
    private Util() {
    }

    private static int length;
    private static int i;


    public static double[][] locationParam(String locationParam) {
        String[] boundary = locationParam.split(",");
        length = boundary.length;
        if (length < 5) {
            throw new SiddhiAppValidationException ("For the location, the bounding box specified is invalid.");
        }
        double[][] locations = new double[length / 2][2];
        int k = 0;
        for (i = 0; i < length / 2; i++) {
            for (int j = 0; j < 2; j++) {
                try {
                    locations[i][j] = Double.parseDouble(boundary[k++]);
                } catch (NumberFormatException e) {
                    throw new SiddhiAppValidationException("Latitude/Longitude should be a double value: "
                            + e.getMessage() , e);
                }
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
                throw new SiddhiAppValidationException("Follow should be a long value: " + e.getMessage() , e);
            }
        }
        return follow;
    }
}

