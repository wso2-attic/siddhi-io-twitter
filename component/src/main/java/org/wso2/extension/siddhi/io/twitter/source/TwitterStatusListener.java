package org.wso2.extension.siddhi.io.twitter.source;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.TwitterObjectFactory;

/**
 * Listens to the public statuses.
 */

public class TwitterStatusListener implements twitter4j.StatusListener {
    private static final Logger log = Logger.getLogger(TwitterConsumer.class);
    private SourceEventListener sourceEventListener;
    private boolean isPaused;

    TwitterStatusListener (SourceEventListener sourceEventListener) {
        this.sourceEventListener = sourceEventListener;
    }

    @Override
    public void onStatus(Status status) {
        if (isPaused) {
            try {
                Thread.sleep(10000);
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

    void pause() {
        isPaused = true;
    }

    void resume() {
        isPaused = false;
    }
}
