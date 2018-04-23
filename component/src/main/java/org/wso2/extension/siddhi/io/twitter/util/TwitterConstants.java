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

import twitter4j.Query;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * {@code TwitterConstants}Twitter Source Constants.
 */

public class TwitterConstants {

    private TwitterConstants() {
    }

    public static final String CONSUMER_KEY = "consumer.key";
    public static final String CONSUMER_SECRET = "consumer.secret";
    public static final String ACCESS_TOKEN = "access.token";
    public static final String ACCESS_SECRET = "access.token.secret";
    public static final String MODE = "mode";
    public static final String MODE_STREAMING = "STREAMING";
    public static final String MODE_POLLING = "POLLING";
    public static final String STREAMING_FILTER_FOLLOW = "follow";
    public static final String STREAMING_FILTER_TRACK = "track";
    public static final String STREAMING_FILTER_LOCATIONS = "location";
    public static final String STREAMING_FILTER_LANGUAGE = "language";
    public static final String STREAMING_FILTER_FILTER_LEVEL = "filter.level";
    public static final String POLLING_SEARCH_COUNT = "count";
    public static final String POLLING_SEARCH_GEOCODE = "geocode";
    public static final String POLLING_SEARCH_QUERY = "query";
    public static final String POLLING_SEARCH_MAXID = "max.id";
    public static final String POLLING_SEARCH_SINCEID = "since.id";
    public static final String POLLING_SEARCH_LANGUAGE = "language";
    public static final String POLLING_SEARCH_RESULT_TYPE = "result.type";
    public static final String POLLING_SEARCH_UNTIL = "until";
    public static final String POLLING_SEARCH_SINCE = "since";
    public static final String POLLING_INTERVAL = "polling.interval";
    private static final String FILTER_LEVEL_NONE = "none";
    private static final String FILTER_LEVEL_MEDIUM = "medium";
    private static final String FILTER_LEVEL_LOW = "low";
    public static final String EMPTY_STRING = "";
    public static final String DELIMITER = ",";
    public static final String NULL_STRING = "null";

    public static final String STATUS_CREATED_AT = "createdAt";
    public static final String STATUS_TWEET_ID = "tweetId";
    public static final String STATUS_TEXT = "text";
    public static final String STATUS_USER_CREATEDAT = "user.createdAt";
    public static final String STATUS_USER_SCREENNAME = "user.screenName";
    public static final String STATUS_USER_NAME = "user.name";
    public static final String STATUS_USER_MAIL = "user.mail";
    public static final String STATUS_USER_ID = "user.id";
    public static final String STATUS_USER_LOCATION = "user.location";
    public static final String STATUS_HASHTAGS = "hashtags";
    public static final String STATUS_USERMENTIONS = "userMentions";
    public static final String STATUS_MEDIAURLS = "mediaUrls";
    public static final String STATUS_URLS = "urls";
    public static final String STATUS_PLACE_COUNTRY = "place.country";
    public static final String STATUS_PLACE_COUNTRY_CODE = "place.country_code";
    public static final String STATUS_PLACE_NAME = "place.name";
    public static final String STATUS_PLACE_ID = "place.id";
    public static final String STATUS_PLACE_FULLNAME = "place.fullName";
    public static final String STATUS_LANGUAGE = "language";
    public static final String STATUS_SOURCE = "source";
    public static final String STATUS_IS_RETWEET = "isRetweet";
    public static final String STATUS_RETWEET_COUNT = "retweetCount";
    public static final String STATUS_GEOLOCATION = "geoLocation";
    public static final String STATUS_FAVOURITE_COUNT = "favouriteCount";
    public static final String STATUS_QUOTED_STATUS_ID = "quotedStatusId";
    public static final String STATUS_IN_REPLY_TO_STATUS_ID = "in.reply.to.status.id";
    public static final String TRACK_WORDS = "track.words";
    public static final String QUERY = "polling.query";

    public static final int MANDATORY_PARAM_SIZE = 6;

    public static final List<String> MANDATORY_PARAM = Collections.unmodifiableList(Arrays.asList(
            TwitterConstants.CONSUMER_KEY,
            TwitterConstants.CONSUMER_SECRET,
            TwitterConstants.ACCESS_TOKEN,
            TwitterConstants.ACCESS_SECRET,
            TwitterConstants.MODE, "type"));

    public static final List<String> STREAMING_PARAM = Collections.unmodifiableList(Arrays.asList(
            TwitterConstants.STREAMING_FILTER_TRACK,
            TwitterConstants.STREAMING_FILTER_FOLLOW,
            TwitterConstants.STREAMING_FILTER_FILTER_LEVEL,
            TwitterConstants.STREAMING_FILTER_LOCATIONS,
            TwitterConstants.STREAMING_FILTER_LANGUAGE));

    public static final List<String> POLLING_PARAM = Collections.unmodifiableList(Arrays.asList(
            TwitterConstants.POLLING_SEARCH_COUNT,
            TwitterConstants.POLLING_SEARCH_QUERY,
            TwitterConstants.POLLING_SEARCH_LANGUAGE,
            TwitterConstants.POLLING_SEARCH_GEOCODE,
            TwitterConstants.POLLING_SEARCH_RESULT_TYPE,
            TwitterConstants.POLLING_SEARCH_MAXID,
            TwitterConstants.POLLING_SEARCH_SINCE,
            TwitterConstants.POLLING_SEARCH_SINCEID,
            TwitterConstants.POLLING_SEARCH_UNTIL,
            TwitterConstants.POLLING_INTERVAL));

    public static final List<String> FILTER_LEVELS = Collections.unmodifiableList(Arrays.asList(
            TwitterConstants.FILTER_LEVEL_LOW,
            TwitterConstants.FILTER_LEVEL_MEDIUM,
            TwitterConstants.FILTER_LEVEL_NONE));

    public static final List<Query.ResultType> RESULT_TYPES = Collections.unmodifiableList(Arrays.asList(
            Query.ResultType.mixed,
            Query.ResultType.popular,
            Query.ResultType.recent));
}
