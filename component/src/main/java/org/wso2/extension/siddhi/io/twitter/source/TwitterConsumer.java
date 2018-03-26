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
import org.wso2.extension.siddhi.io.twitter.util.Util;
import org.wso2.siddhi.core.config.SiddhiAppContext;
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

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * This class handles consuming tweets and passing to the stream.
 */

public enum TwitterConsumer {
    INSTANCE;

    private static final Logger log = Logger.getLogger(TwitterConsumer.class);
    private boolean isPaused = false;
    private int sleepTime = 10000;
    Query query;

    /**
     * This method handles consuming livestream tweets.
     *
     * @param twitterStream       - For streaming mode
     * @param sourceEventListener - Listen events
     * @param languageParam       - Specifies language
     * @param trackParam          - Specifies keyword to track
     * @param follow              - Specifies follower's id
     * @param filterLevel         - Specifies filter level( low ,medium, none)
     * @param locations           - Specifies location
     */

    public void consume(TwitterStream twitterStream, SourceEventListener sourceEventListener,
                        String languageParam, String trackParam, long[] follow,
                        String filterLevel, double[][] locations, int paramSize) {
        FilterQuery filterQuery;
        String[] tracks;
        String[] filterLang;

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                if (isPaused) {
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Thread was interrupted during sleep : " + ie);
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
            tracks = Util.extract(trackParam);
            filterQuery.track(tracks);
        }

        if (!languageParam.trim().isEmpty()) {
            filterLang = Util.extract(languageParam);
            filterQuery.language(filterLang);
        }

        if (follow != null) {
            filterQuery.follow(follow);
        }

        if (!filterLevel.trim().isEmpty()) {
            filterQuery.filterLevel(filterLevel);
        }

        if (locations != null) {
            filterQuery.locations(locations);
        }

        if (paramSize == 6) {
            twitterStream.sample();
        } else {
            twitterStream.filter(filterQuery);
        }
    }

    /**
     * This method handles consuming historical tweets within a week.
     *
     * @param twitter             - For Twitter Polling
     * @param sourceEventListener - Listen Events
     */

    public void consume(Twitter twitter, Query query, SourceEventListener sourceEventListener, SiddhiAppContext
            siddhiAppContext, long pollingInterval, long tweetId) {
        ExecutorService executorService = siddhiAppContext.getExecutorService();
        executorService.execute(new Polling(twitter, query, sourceEventListener, pollingInterval, tweetId));
    }

    public Query createQuery (String q, int count, String language, long sinceId, long maxId, String until,
                              String since, String resultType, String geoCode, double latitude, double longitude,
                              double radius, String unitName) {
        query = new Query(q);
        if (count > 0) {
            query.count(count);
        }
        if (!language.trim().isEmpty()) {
            query.lang(language);
        }
        if (!resultType.trim().isEmpty()) {
            query.resultType(Query.ResultType.valueOf(resultType));
        }
        if (!geoCode.trim().isEmpty()) {
            query.geoCode(new GeoLocation(latitude, longitude), radius, unitName);
        }
        if (!until.trim().isEmpty()) {
            query.until(until);
        }
        if (!since.trim().isEmpty()) {
            query.since(since);
        }
        query.sinceId(sinceId);
        query.maxId(maxId);

        return query;
    }

    public void pause() {
        isPaused = true;
    }

    public void resume() {
        isPaused = false;
    }

    /**
     * This class is for polling tweets continuously.
     */

    static class Polling implements Runnable {
        Twitter twitter;
        Query query;
        QueryResult result;
        SourceEventListener sourceEventListener;
        long pollingInterval;
        long tweetId;


        Polling(Twitter twitter, Query query, SourceEventListener sourceEventListener, long pollingInterval,
                long tweetId) {
            this.twitter = twitter;
            this.query = query;
            this.sourceEventListener = sourceEventListener;
            this.pollingInterval = pollingInterval;
            this.tweetId = tweetId;
        }

        @Override
        public void run() {
            int i = 0;
            Query query1 = query;
            while (true) {
                boolean flag = true;
                do {
                    try {
                        result = twitter.search(query);
                        List<Status> tweets = result.getTweets();
                        for (Status tweet : tweets) {
                            if (flag) {
                                tweetId = tweet.getId();
                                flag = false;
                            }
                            log.info(++i + "\n" + "---------------------------------------------");
                            sourceEventListener.onEvent(TwitterObjectFactory.getRawJSON(tweet), null);
                        }
                        query = result.nextQuery();
                        checkRateLimit(result);
                    } catch (TwitterException te) {
                        log.error("Failed to search tweets: " + te.getMessage());
                    }
                } while (result.hasNext());
                try {
                    Thread.sleep(pollingInterval);
                    query = query1;
                    query.setSinceId(tweetId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Thread was interrupted during sleep or wait : " + e);
                }
            }
        }

        private void checkRateLimit(QueryResult result) {
            if (result.getRateLimitStatus().getRemaining() <= 0) {
                try {
                    Thread.sleep(result.getRateLimitStatus().getSecondsUntilReset());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Thread was interrupted during sleep or wait : " + e);
                }
            }
        }
    }
}

