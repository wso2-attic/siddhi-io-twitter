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

import io.siddhi.core.stream.input.source.SourceEventListener;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.twitter.util.Util;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Listens Twitter status and pass the events to the stream.
 */

public class TwitterStatusListener implements StatusListener {
    private static final Logger log = Logger.getLogger(TwitterStatusListener.class);
    private SourceEventListener sourceEventListener;
    private boolean paused;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    TwitterStatusListener(SourceEventListener sourceEventListener) {
        this.sourceEventListener = sourceEventListener;
    }

    @Override
    public void onStatus(Status status) {
        Map<String, Object> event;
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
        event = Util.createMap(status);
        sourceEventListener.onEvent(event, null);
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

    public void pause() {
        paused = true;
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
