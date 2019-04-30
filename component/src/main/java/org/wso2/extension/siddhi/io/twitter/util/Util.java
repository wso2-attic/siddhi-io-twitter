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

import io.siddhi.query.api.exception.SiddhiAppValidationException;
import twitter4j.Status;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains util methods for the extensions.
 */
public class Util {
    private Util() {
    }


    public static double[][] locationParam(String locationParam) {
        String[] boundary = locationParam.split(TwitterConstants.DELIMITER);
        if (boundary.length < 5) {
            throw new SiddhiAppValidationException("For the location, the bounding box specified is invalid.");
        }
        double[][] locations = new double[boundary.length / 2][2];
        int k = 0;
        for (int i = 0; i < boundary.length / 2; i++) {
            for (int j = 0; j < 2; j++) {
                try {
                    locations[i][j] = Double.parseDouble(boundary[k++]);
                } catch (NumberFormatException e) {
                    throw new SiddhiAppValidationException("Latitude/Longitude should be a double value: "
                            + e.getMessage(), e);
                }
            }
        }
        return locations;
    }

    public static long[] followParam(String followParam) {
        long[] follow;
        follow = new long[followParam.split(TwitterConstants.DELIMITER).length];
        for (int i = 0; i < followParam.split(TwitterConstants.DELIMITER).length; i++) {
            try {
                follow[i] = Long.parseLong(followParam.split(TwitterConstants.DELIMITER)[i]);
            } catch (NumberFormatException e) {
                throw new SiddhiAppValidationException("Follow should be a long value: " + e.getMessage(), e);
            }
        }
        return follow;
    }

    public static Map<String, Object> createMap(Status tweet) {
        Map<String, Object> status = new HashMap<>();
        String trackwords = (QueryBuilder.getTrackParam() != null && !QueryBuilder.getTrackParam().isEmpty()) ?
                QueryBuilder.getTrackParam() : TwitterConstants.NULL_STRING;
        String query = (QueryBuilder.getQueryParam() != null && !QueryBuilder.getQueryParam().isEmpty()) ?
                QueryBuilder.getQueryParam() : TwitterConstants.NULL_STRING;
        String geoLocation = (tweet.getGeoLocation() == null ?
                TwitterConstants.NULL_STRING : (tweet.getGeoLocation().getLatitude()) + TwitterConstants.DELIMITER +
                tweet.getGeoLocation().getLongitude());
        StringBuilder hashtag = new StringBuilder();
        StringBuilder userMention = new StringBuilder();
        StringBuilder mediaUrl = new StringBuilder();
        StringBuilder url = new StringBuilder();
        String createdAt;
        String hashtags;
        String userMentions;
        String mediaUrls;
        String urls;
        int i;
        for (i = 0; i < tweet.getHashtagEntities().length; i++) {
            hashtag.append(tweet.getHashtagEntities()[i].getText() + TwitterConstants.DELIMITER);
        }
        hashtags = hashtag.toString();
        hashtags = (hashtags.equals(TwitterConstants.EMPTY_STRING) ?
                TwitterConstants.NULL_STRING : hashtags.substring(0, hashtags.length() - 1));

        for (i = 0; i < tweet.getUserMentionEntities().length; i++) {
            userMention.append(tweet.getUserMentionEntities()[i].getText() + TwitterConstants.DELIMITER);
        }
        userMentions = userMention.toString();
        userMentions = (userMentions.equals(TwitterConstants.EMPTY_STRING) ?
                TwitterConstants.NULL_STRING : userMentions.substring(0,
                userMentions.length() - 1));

        for (i = 0; i < tweet.getMediaEntities().length; i++) {
            mediaUrl.append(tweet.getMediaEntities()[i].getMediaURL() + TwitterConstants.DELIMITER);
        }
        mediaUrls = mediaUrl.toString();
        mediaUrls = (mediaUrls.equals(TwitterConstants.EMPTY_STRING) ?
                TwitterConstants.NULL_STRING : mediaUrls.substring(0,
                mediaUrls.length() - 1));
        for (i = 0; i < tweet.getURLEntities().length; i++) {
            url.append(tweet.getURLEntities()[i].getURL() + TwitterConstants.DELIMITER);
        }
        urls = url.toString();
        urls = (urls.equals(TwitterConstants.EMPTY_STRING) ?
                TwitterConstants.NULL_STRING : urls.substring(0,
                urls.length() - 1));
        createdAt = tweet.getCreatedAt().toString();

        status.put(TwitterConstants.STATUS_CREATED_AT, createdAt);
        status.put(TwitterConstants.STATUS_TWEET_ID, tweet.getId());
        status.put(TwitterConstants.STATUS_TEXT, tweet.getText());
        status.put(TwitterConstants.STATUS_USER_CREATEDAT, tweet.getUser().getCreatedAt());
        status.put(TwitterConstants.STATUS_USER_SCREENNAME, tweet.getUser().getScreenName());
        status.put(TwitterConstants.STATUS_USER_NAME, tweet.getUser().getName());
        status.put(TwitterConstants.STATUS_USER_MAIL, tweet.getUser().getEmail());
        status.put(TwitterConstants.STATUS_USER_ID, tweet.getUser().getId());
        status.put(TwitterConstants.STATUS_USER_LOCATION, tweet.getUser().getLocation());
        status.put(TwitterConstants.STATUS_HASHTAGS, hashtags);
        status.put(TwitterConstants.STATUS_USERMENTIONS, userMentions);
        status.put(TwitterConstants.STATUS_MEDIAURLS, mediaUrls);
        status.put(TwitterConstants.STATUS_URLS, urls);
        status.put(TwitterConstants.STATUS_LANGUAGE, tweet.getLang());
        status.put(TwitterConstants.STATUS_SOURCE, tweet.getSource());
        status.put(TwitterConstants.STATUS_IS_RETWEET, tweet.isRetweet());
        status.put(TwitterConstants.STATUS_RETWEET_COUNT, tweet.getRetweetCount());
        status.put(TwitterConstants.STATUS_GEOLOCATION, geoLocation);
        status.put(TwitterConstants.STATUS_FAVOURITE_COUNT, tweet.getFavoriteCount());
        status.put(TwitterConstants.STATUS_QUOTED_STATUS_ID, tweet.getQuotedStatusId());
        status.put(TwitterConstants.STATUS_IN_REPLY_TO_STATUS_ID, tweet.getInReplyToStatusId());
        status.put(TwitterConstants.STATUS_PLACE_COUNTRY, (tweet.getPlace() == null) ?
                TwitterConstants.NULL_STRING : tweet.getPlace().getCountry());
        status.put(TwitterConstants.STATUS_PLACE_COUNTRY_CODE, (tweet.getPlace() == null) ?
                TwitterConstants.NULL_STRING : tweet.getPlace().getCountryCode());
        status.put(TwitterConstants.STATUS_PLACE_NAME, (tweet.getPlace() == null) ?
                TwitterConstants.NULL_STRING : tweet.getPlace().getName());
        status.put(TwitterConstants.STATUS_PLACE_ID, (tweet.getPlace() == null) ?
                TwitterConstants.NULL_STRING : tweet.getPlace().getId());
        status.put(TwitterConstants.STATUS_PLACE_FULLNAME, (tweet.getPlace() == null) ?
                TwitterConstants.NULL_STRING : tweet.getPlace().getFullName());
        status.put(TwitterConstants.TRACK_WORDS, trackwords);
        status.put(TwitterConstants.QUERY, query);
        return status;
    }
}
