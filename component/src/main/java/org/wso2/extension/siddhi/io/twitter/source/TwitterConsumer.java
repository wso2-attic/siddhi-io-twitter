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

package org.wso2.extension.siddhi.io.twitter.source;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;

import java.util.ArrayList;
import java.util.List;

/**
 * This class handles consuming livestream tweets .
 */

public class TwitterConsumer {
    private static final Logger log = Logger.getLogger(TwitterSource.class);
    private static boolean isPaused = false;


    /**
     * @param twitterStream       - For streaming mode
     * @param sourceEventListener - Listen events
     * @param languageParam       - Specifies language
     * @param trackParam          - Specifies keyword to track
     * @param followParam         - Specifies follower's id
     * @param filterLevel         - Specifies filter level( low ,medium, none)
     * @param locationParam       - Specifies location
     */
    public static void consume(TwitterStream twitterStream, SourceEventListener sourceEventListener,
                               String languageParam, String trackParam, String followParam,
                               String filterLevel, String locationParam) {
        FilterQuery filterQuery;
        String[] tracks;
        String[] filterLang;
        long[] follow;
        String[] locationPair;
        double[][] locations;
        int i;
        int length;

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                    if (isPaused) {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                sourceEventListener.onEvent(TwitterObjectFactory.getRawJSON(status), null);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                log.debug("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                log.debug("Got track limitation notice: " + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                log.debug("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                log.debug("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                log.error("Twitter source threw an exception", ex);
            }
        };

        twitterStream.addListener(listener);
        filterQuery = new FilterQuery();
        if (!trackParam.trim().isEmpty()) {
            tracks = extract(trackParam);
            filterQuery.track(tracks);
        }

        if (!languageParam.trim().isEmpty()) {
            filterLang = extract(languageParam);
            filterQuery.language(filterLang);
        }

        if (!followParam.trim().isEmpty()) {
            length = followParam.split(",").length;
            follow = new long[length];
            for (i = 0; i < length; i++) {
                follow[i] = Long.parseLong(followParam.split(",")[i]);
            }
            filterQuery.follow(follow);
        }

        if (!filterLevel.trim().isEmpty()) {
            filterQuery.filterLevel(filterLevel);
        }

        if (!locationParam.trim().isEmpty()) {
            length = locationParam.split(",").length;
            locationPair = new String[length];
            for (i = 0; i < length; ++i) {
                locationPair[i] = locationParam.split(",")[i];
            }
            length = locationPair.length;
            locations = new double[length][2];

            for (i = 0; i < locationPair.length; ++i) {
                locations[i][0] = Double.parseDouble(locationPair[i].split(":")[0]);
                locations[i][1] = Double.parseDouble(locationPair[i].split(":")[1]);
            }
            filterQuery.locations(locations);
        }

        if (followParam.trim().isEmpty() && trackParam.trim().isEmpty() &&
                languageParam.trim().isEmpty() && locationParam.trim().isEmpty()) {
            twitterStream.sample();
        } else {
            twitterStream.filter(filterQuery);
        }
    }

    /**
     * This method handles consuming past tweets within a week
     *
     * @param twitter             - For Twitter Polling
     * @param sourceEventListener - Listen Events
     * @param q                   - Defines search query
     * @param language            - Restricts tweets to the given language
     * @param sinceId             - Returns results with an ID greater than the specified ID.
     * @param maxId               - Returns results with an ID less than or equal to the specified ID.
     * @param until               - Returns tweets created before the given date.
     * @param resultType          - Specifies what type of search results you would prefer to receive.
     * @param geoCode             - Returns tweets by users located within a given radius of the given
     *                            latitude/longitude.
     */

    public static void consume (Twitter twitter, SourceEventListener sourceEventListener, String q, String language,
                               long sinceId, long maxId, String until, String resultType, String geoCode)
            throws InterruptedException {
        try {
            Query query = new Query(q);
            QueryResult result;
            if (!language.trim().isEmpty()) {
                query.lang(language);
            }
            query.sinceId(sinceId);
            query.maxId(maxId);
            if (!until.trim().isEmpty()) {
                query.until(until);
            }
            if (!resultType.trim().isEmpty()) {
                query.resultType(Query.ResultType.valueOf(resultType));
            }
            if (!geoCode.trim().isEmpty()) {
                String[] parts = extract(geoCode);
                double latitude = Double.parseDouble(parts[0]);
                double longitude = Double.parseDouble(parts[1]);
                double radius = 0.0;
                Query.Unit unit = null;
                String radiusstr = parts[2];
                for (Query.Unit value : Query.Unit.values()) {
                    if (radiusstr.endsWith(value.name())) {
                        radius = Double.parseDouble(radiusstr.substring(0, radiusstr.length() - 2));
                        unit = value;
                        break;
                    }
                }
                if (unit == null) {
                    throw new IllegalArgumentException("Unrecognized geocode radius: " + radiusstr);
                }
                String unitName = unit.name();
                query.geoCode(new GeoLocation(latitude, longitude), radius, unitName);
            }

            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                    if (isPaused) {
                        Thread.sleep(10000);
                    }
                    sourceEventListener.onEvent(TwitterObjectFactory.getRawJSON(tweet), null);
                }
            } while ((query = result.nextQuery()) != null);
        } catch (TwitterException te) {
            log.error("Failed to search tweets: " + te.getMessage());
        }
    }

    /**
     * Validates the parameters that allows specific values.
     *
     * @param mode        - Streaming or polling mode
     * @param filterLevel - Specifies filter level( low ,medium, none)
     * @param resultType  - Specifies what type of search results you would prefer to receive.
     */
    public static void validateParameter(String mode, String query, String filterLevel, String resultType) {
        ArrayList<String> filterLevels = new ArrayList();
        filterLevels.add("none");
        filterLevels.add("medium");
        filterLevels.add("low");
        ArrayList<String> resultTypes = new ArrayList();
        resultTypes.add("mixed");
        resultTypes.add("popular");
        resultTypes.add("recent");

        if (mode.equalsIgnoreCase("STREAMING") || mode.equalsIgnoreCase("POLLING")) {
            if (mode.equalsIgnoreCase("STREAMING")) {
                if (log.isDebugEnabled()) {
                    log.debug("In Streaming mode, you can only give these following parameters. " +
                            "If you give any other parameters, they will be ignored.\n" +
                            "{track, language, follow, location, filterlevel}");
                }
            } else {
                if (query.isEmpty()) {
                    throw new SiddhiAppCreationException("In polling mode, query is a mandatory parameter.");
                }

                if (log.isDebugEnabled()) {
                    log.debug("In polling mode, you can only give these following parameters. " +
                            "If you give any other parameters, they will be ignored.\n" +
                            "{query, geocode, max.id, since.id, language, result.type, until}");
                }
            }
        } else {
            throw new SiddhiAppCreationException("There are only two possible values for mode :" +
                    " streaming or polling. But found '" + mode + "'.");
        }

        if (!filterLevels.contains(filterLevel)) {
            throw new SiddhiAppCreationException("There are only three possible values for filterlevel :" +
                    " low or medium or none. But found '" + filterLevel + "'.");
        }

        if (!resultTypes.contains(resultType)) {
            throw new SiddhiAppCreationException("There are only three possible values for result.type :" +
                    " mixed or popular or recent. But found '" + resultType + "'.");
        }
    }

    public static void pause() {
        isPaused = true;
    }

    public static void resume() {
        isPaused = false;
    }

    public static String[] extract (String str) {
       return str.split(",");
    }
}

