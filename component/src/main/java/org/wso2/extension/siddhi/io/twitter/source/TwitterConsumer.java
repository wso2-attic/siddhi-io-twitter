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
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import twitter4j.FilterQuery;
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
    private boolean isPaused;
    private int sleepTime = 10000;

    /**
     * This method handles consuming livestream tweets.
     */

    public void consume(TwitterStream twitterStream, SourceEventListener sourceEventListener, FilterQuery filterQuery,
                        int paramSize) {
        twitterStream.addListener(new TwitterStatusListener(sourceEventListener));
        if (paramSize == 6) {
            twitterStream.sample();
        } else {
            twitterStream.filter(filterQuery);
        }
    }

    /**
     * This method handles consuming historical tweets within a week.
     *
     * @param twitter             - For Twitter TwitterPoller
     * @param sourceEventListener - Listen Events
     */

    public void consume(Twitter twitter, Query query, SourceEventListener sourceEventListener, SiddhiAppContext
            siddhiAppContext, long pollingInterval, long tweetId) {
        ExecutorService executorService = siddhiAppContext.getExecutorService();
        executorService.execute(new TwitterPoller(twitter, query, sourceEventListener, pollingInterval, tweetId));
    }

    /**
     * This class is for polling tweets continuously.
     */

    class TwitterPoller implements Runnable {
        Twitter twitter;
        Query query;
        QueryResult result;
        SourceEventListener sourceEventListener;
        long pollingInterval;
        long tweetId;


        /**
         * Handles polling in a different thread.
         * @param twitter - twitter instance
         * @param query - specifies a query
         * @param sourceEventListener - listens events
         * @param pollingInterval - specifies the interval to poll periodically
         * @param tweetId - update the latest tweet id
         */
        TwitterPoller(Twitter twitter, Query query, SourceEventListener sourceEventListener, long pollingInterval,
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
            Query qry = query;
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
                            if (isPaused) {
                                sleep(sleepTime);
                            }
                            log.info(++i + "------------------------------------------");
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
                    query = qry;
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

    /**
     * listens to the public statuses.
     */
    public class TwitterStatusListener implements StatusListener {
        private SourceEventListener sourceEventListener;

        TwitterStatusListener(SourceEventListener sourceEventListener) {
            this.sourceEventListener = sourceEventListener;
        }

        @Override
        public void onStatus(Status status) {
            if (isPaused) {
                sleep(sleepTime);
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
    }

    public void pause() {
        isPaused = true;
    }

    public void resume() {
        isPaused = false;
    }

    public void sleep (int sleepTime) {
        try {
            while (!isPaused) {
                Thread.sleep(sleepTime);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("Thread was interrupted during sleep : " + ie);
        }
    }
}

