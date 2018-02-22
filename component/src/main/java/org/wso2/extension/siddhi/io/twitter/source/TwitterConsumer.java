package org.wso2.extension.siddhi.io.twitter.source;

import org.apache.log4j.Logger;
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

/**
 * This class handles consuming livestream tweets .
 */

public class TwitterConsumer {
    private static final Logger log = Logger.getLogger(TwitterSource.class);

    private static String[] tracks;
    private static String[] filterLang;
    private static long[] follow;
    private static String[] locationPair;
    private static double[][] locations;
    private static FilterQuery filterQuery;
    private static int i;
    private static int length;


    /**
     * this class is for twitter consuming.
     *
     * @param languageParam
     * @param trackParam
     * @return
     * @throws Exception
     */


    public static void consume(TwitterStream twitterStream, SourceEventListener sourceEventListener,
                               String languageParam, String trackParam, String followParam,
                               String filterLevel, String locationParam) {
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
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
        if (!trackParam.isEmpty()) {
            tracks = trackParam.split(",");
            filterQuery.track(tracks);
        }

        if (!languageParam.isEmpty()) {
            filterLang = languageParam.split(",");
            filterQuery.language(filterLang);
        }

        if (!followParam.isEmpty()) {
            length = followParam.split(",").length;
            follow = new long[length];
            for (i = 0; i < length; i++) {
                follow[i] = Long.parseLong(followParam.split(",")[i]);
            }
            filterQuery.follow(follow);
        }

        if (!filterLevel.isEmpty()) {
            filterQuery.filterLevel(filterLevel);
        }

        if (!locationParam.isEmpty()) {
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

        if (followParam.isEmpty() && trackParam.isEmpty() &&
                languageParam.isEmpty() && locationParam.isEmpty()) {
            twitterStream.sample();
        } else {
            twitterStream.filter(filterQuery);
        }
    }

    /**
     * This method handles consuming past tweets within a week
     *
     * @param twitter
     * @param sourceEventListener
     * @param q
     * @param language
     * @param sinceId
     * @param maxId
     * @param until
     * @param resultType
     * @param geoCode
     * @throws Exception
     */

    public static void consume(Twitter twitter, SourceEventListener sourceEventListener, String q, String language,
                               long sinceId, long maxId, String until, String resultType, String geoCode) {
        try {
            Query query = new Query(q);
            QueryResult result;
            query.lang(language);
            query.sinceId(sinceId);
            query.maxId(maxId);
            query.until(until);
            if (!resultType.isEmpty()) {
                query.resultType(Query.ResultType.valueOf(resultType));
            }
            if (!geoCode.isEmpty()) {
                String[] parts = geoCode.split(",");
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
                    throw new IllegalArgumentException("unrecognized geocode radius: " + radiusstr);
                }
                String unitName = unit.name();
                query.geoCode(new GeoLocation(latitude, longitude), radius, unitName);
            }

            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                    sourceEventListener.onEvent(TwitterObjectFactory.getRawJSON(tweet), null);
                }
            } while ((query = result.nextQuery()) != null);
        } catch (TwitterException te) {
            log.error("Failed to search tweets: " + te.getMessage());
        }
    }
}
