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
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is for polling tweets continuously.
 */
public class TwitterPoller implements Runnable {
    private static final Logger log = Logger.getLogger(TwitterPoller.class);
    private boolean paused;
    private boolean isKilled = false;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private Twitter twitter;
    private Query query;
    private QueryResult result;
    private SourceEventListener sourceEventListener;
    long tweetId = -1;

    /**
     * Handles polling in a different thread.
     *
     * @param twitter             - twitter instance
     * @param query               - specifies a query
     * @param sourceEventListener - listens events
     */
    TwitterPoller(Twitter twitter, Query query, SourceEventListener sourceEventListener) {
        this.twitter = twitter;
        this.query = query;
        this.sourceEventListener = sourceEventListener;
    }

    @Override
    public void run() {
        Map<String, Object> event;
        boolean isLatestId = true;
        do {
            try {
                if (isKilled) {
                    return;
                }
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                    event = Util.createMap(tweet);
                    if (isLatestId) {
                        tweetId = tweet.getId();
                        isLatestId = false;
                    }
                    if (paused) { //spurious wakeup condition is deliberately traded off for performance
                        lock.lock();
                        try {
                            while (paused) {
                                condition.await();
                            }
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        } finally {
                            lock.unlock();
                        }
                    }
                    sourceEventListener.onEvent(event, null);
                }
                if (result.nextQuery() != null) {
                    query = result.nextQuery();
                } else {
                    query.setSinceId(tweetId);
                    query.setMaxId(-1);
                }
                checkRateLimit(result);
            } catch (TwitterException te) {
                log.error("Failed to search tweets: " + te.getMessage());
            }
        } while (result.nextQuery() != null);
    }

    /**
     * Checks the number of the remaining requests within window and wait
     * until window will be reset.
     *
     * @param result - Results of the specified Query.
     */
    private void checkRateLimit(QueryResult result) {
        if (result.getRateLimitStatus().getRemaining() <= 0) {
            try {
                Thread.sleep(result.getRateLimitStatus().getSecondsUntilReset());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread was interrupted during wait : " + e);
            }
        }
    }

    public void pause() {
        paused = true;
    }

    public void kill() {
        isKilled = true;
    }

    public void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
