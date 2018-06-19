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
import org.wso2.extension.siddhi.io.twitter.util.QueryBuilder;
import org.wso2.extension.siddhi.io.twitter.util.TwitterConstants;
import org.wso2.extension.siddhi.io.twitter.util.Util;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Twitter Source Implementation
 */
@Extension(
        name = "twitter",
        namespace = "source",
        description = "The Twitter source receives events from a Twitter app. The events are received " +
                "in the form of key-value mappings. \n\n" +
                "The following are key values of the map of a tweet and their descriptions:\n\t" +
                "1.  createdAt: The UTC time at which the Tweet was created.\n\t" +
                "2.  tweetId: The integer representation for the unique identifier of the Tweet.\n\t" +
                "3.  text: The actual UTF-8 text of the status update.\n\t" +
                "4.  user.createdAt: The UTC date and time at which the user account was created on Twitter.\n\t" +
                "5.  user.id: The integer representation for the unique identifier of the user who posted the " +
                "Tweet.\n\t" +
                "6.  user.screenName: The screen name with which the user identifies himself/herself.\n\t" +
                "7.  user.name: The name of the user (as specified by the user).\n\t" +
                "8.  user.mail: The `mail.id` of the user.\n\t" +
                "9.  user.location: The location in which the current user account profile is saved. This " +
                "parameter can have a null value.\n\t" +
                "10. hashtags: The hashtags that have been parsed out of the Tweet.\n\t" +
                "11. userMentions: The other Twitter users who are mentioned in the text of the Tweet.\n\t" +
                "12. mediaUrls: The media elements uploaded with the Tweet.\n\t" +
                "13. urls: The URLs included in the text of a Tweet.\n\t" +
                "14. language: The language in which the Tweet is posted.\n\t" +
                "15. source: the utility used to post the Tweet as an HTML-formatted string.\n\t" +
                "16. isRetweet: This indicates whether the Tweet is a Retweet or not.\n\t" +
                "17. retweetCount: The number of times the Tweet has been retweeted.\n\t" +
                "18. favouriteCount: This indicates the number of times the Tweet has been liked by Twitter users." +
                " The value for this field can be null.\n\t" +
                "19. geoLocation: The geographic location from which the Tweet was posted by the user or client " +
                "application. The value for this field can be null.\n\t" +
                "20. quotedStatusId: This field appears only when the Tweet is a quote Tweet. It displays " +
                "the integer value Tweet ID of the quoted Tweet.\n\t" +
                "21. in.reply.to.status.id: If the Tweet is a reply to another Tweet, this field displays the " +
                "integer representation of the original Tweet's ID. The value for this field can be null.\n\t" +
                "22. place.id: An ID representing the current location from which the Tweet is read. This is " +
                "represented as a string and not an integer.\n\t" +
                "23. place.name: A short, human-readable representation of the name of the place.\n\t" +
                "24. place.fullName: A complete human-readable representation of the name of the place.\n\t" +
                "25. place.country_code: A shortened country code representing the country in which the place " +
                "is located.\n\t" +
                "26. place.country: The name of the country in which the place is located.\n\t" +
                "27. track.words: The keywords given by the user to track.\n\t" +
                "28. polling.query: The query provided by the user.\n\t",
        parameters = {
                @Parameter(
                        name = "consumer.key",
                        description = "The API key to access the Twitter application created.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "consumer.secret",
                        description = "The API secret to access the Twitter application created.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "access.token",
                        description = "The access token to be used to make API requests on behalf of your account.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "access.token.secret",
                        description = "The access token secret to be used to make API requests on behalf of your" +
                                " account.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "mode",
                        description = "The mode in which the Twitter application is run. Possible values are as " +
                                "follows: \n" +
                                "`streaming`: This retrieves real time tweets. \n2" +
                                "`polling`: This retrieves historical tweets that were posted within one week.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "filter.level",
                        description = "This is assigned to Tweets based on the level of engagement. The filter " +
                                "level can be `none`, `low`, or `medium`. The highest level (i.e., `medium`) " +
                                "corresponds loosely with the `top tweets` filter that the service already offers " +
                                "in its on-site search function.",
                        optional = true,
                        defaultValue = "none",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "track",
                        description = "This filters the Tweets that include the specified keywords.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "follow",
                        description = "This filters the Tweets that are tweeted by the specified user IDs.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.LONG}),
                @Parameter(
                        name = "location",
                        description = "This filters Tweets based on the locations. Here, you need to specify the" +
                                "latitude and the longitude of the location e.g., `51.683979:0.278970`.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.DOUBLE}),
                @Parameter(
                        name = "language",
                        description = "This filters Tweets that are posted in the specified language, given by an" +
                                " ISO 639-1 code.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "query",
                        description = "This filters Tweets that match the specified UTF-8, URL-encoded search" +
                                " query with a maximum of 500 characters including operators. \n e.g., " +
                                "'@NASA' - mentioning Twitter account 'NASA'.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "count",
                        description = "This returns a specified number of Tweets per page up to a maximum of 100.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "geocode",
                        description = "This returns Tweets by users who are located within a specified radius of " +
                                "the given latitude/longitude. The location is preferentially taken from " +
                                "the Geotagging API, but it falls back to their Twitter profile. The parameter value" +
                                " is specified in the `latitude,longitude,radius` format where theradius units must" +
                                " be specified as either `mi` (miles) or `km` (kilometers).",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "result.type",
                        description = "This parameter allows to to specify the whether you want to receive only " +
                                "popular Tweets, the most recent Tweets or a mix of both." +
                                "The possible values are as follows:\n" +
                                "* `mixed`: This includes both popular and recent results in the response.\n" +
                                "* `recent`: This includes only the most recent results in the response.\n" +
                                "* `popular`: This includes only the most popular results in the response.)",
                        optional = true,
                        defaultValue = "mixed",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "max.id",
                        description = "This returns Tweets of which the Tweet ID is equal to or less than (i.e., " +
                                "older than) the specified ID",
                        optional = true,
                        defaultValue = "-1",
                        type = {DataType.LONG}),
                @Parameter(
                        name = "since.id",
                        description = "This returns Tweets of which the Tweet ID is greater than (i.e., more " +
                                "recent than) the specified ID.",
                        optional = true,
                        defaultValue = "-1",
                        type = {DataType.LONG}),
                @Parameter(
                        name = "until",
                        description = "This returns Tweets that were created before the given date. Date needs to be" +
                                " formatted as `YYYY-MM-DD`. The search index has a 7-day limit. Therefore, it is " +
                                "not possible to return Tweets that were created more than a week before the current" +
                                " date.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "polling.interval",
                        description = "This specifies a time interval (in seconds) to poll the Tweets periodically.",
                        optional = true,
                        defaultValue = "3600",
                        type = {DataType.LONG}),
        },
        examples = {
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'streaming', " +
                                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId'," +
                                " text= 'text',hashtags = 'hashtags'))) \n" +
                                "define stream inputStream(createdAt String, id long, text String, hashtags string);",
                        description = "In this example, the twitter source starts listening to a random " +
                                "sample of public statuses and passes the events to the `rcvEvents` stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'streaming'" +
                                ", track = 'Amazon,Google,Apple', language = 'en', @map(type='keyvalue', " +
                                "@attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                                "hashtags = 'hashtags'))) \n" +
                                "define stream inputStream(createdAt String, id long, text String, hashtags string);",
                        description = "In this example, the twitter source starts listening to Tweets in English " +
                                "that include the keywords `Amazon`, `google`, or `apple`. Then these Tweets are" +
                                " passed to the `rcvEvents` stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'streaming'" +
                                ", track = 'Amazon,Google,Apple', language = 'en', filter.level = 'low', " +
                                "follow = '11348282,20536157,15670515,17193794,58561993,18139619'," +
                                "location = '51.280430:-0.563160,51.683979:0.278970', @map(type='keyvalue', " +
                                "@attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                                "hashtags = 'hashtags'))) \n" +
                                "define stream inputStream(createdAt String, id long, text String, hashtags string);",
                        description = "In this example, the twitter source starts listening to Tweets in English " +
                                "that either include the keywords `Amazon`, `google`, `apple`, tweeted by the " +
                                "specified followers, or tweeted from the given location based on the filter.level." +
                                " Then these Tweets are passed to the `rcvEvents` stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'polling'" +
                                ", query = 'happy hour', @map(type='keyvalue', @attributes(createdAt = 'createdAt'," +
                                " id = 'tweetId', text= 'text', hashtags = 'hashtags'))) \n" +
                                "define stream inputStream(createdAt String, id long, text String, hashtags string);",
                        description = "In this example, the twitter source starts polling Tweets that contain the" +
                                " exact phrase `happy hour`. Then these Tweets are passed to the `rcvEvents` stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'polling'" +
                                ", query = '#Amazon', since.id = '973439483906420736', @map(type='keyvalue', " +
                                "@attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                                "hashtags = 'hashtags'))) \n" +
                                "define stream inputStream(createdAt String, id long, text String, hashtags string);",
                        description = "In this example, the twitter source starts polling tweets that contain the" +
                                " `#Amazon` hashtag and have a Tweet Id that is greater than `since.id`. Then these" +
                                " Tweets are passed to the `rcvEvents` stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'polling'" +
                                ", query = '@NASA', language = 'en', result.type = 'recent'," +
                                " geocode = '43.913723261972855,-72.54272478125,150km', " +
                                "since.id = 24012619984051000, max.id = 250126199840518145, until = 2018-03-10," +
                                " @map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', " +
                                "text= 'text', hashtags = 'hashtags'))) \n" +
                                "define stream inputStream(createdAt String, id long, text String, hashtags string);",
                        description = "In this example, the twitter source starts polling the recent Tweets in " +
                                "English that mention `NASA`, and have Tweet IDs that are greater than the " +
                                "`since.id` and less than the `max.id`. Then these events are passed to the " +
                                "`rcvEvents` stream."
                )
        }
)

public class TwitterSource extends Source {
    private static final Logger log = Logger.getLogger(TwitterSource.class);
    private TwitterPoller twitterPoller;
    private TwitterStatusListener twitterStatusListener;
    private SourceEventListener sourceEventListener;
    private TwitterStream twitterStream;
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessSecret;
    private String mode;
    private String followParam;
    private String locationParam;
    private String trackParam;
    private String languageParam;
    private String filterLevel;
    private String queryParam;
    private int count;
    private String geocode;
    private long maxId;
    private Long sinceId;
    private String searchLang;
    private String until;
    private String since;
    private String resultType;
    private long pollingInterval;
    private long[] follow;
    private double[][] locations;
    private double latitude;
    private double longitude;
    private double radius;
    private String unitName;
    private Set<String> staticOptionsKeys;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture scheduledFuture;


    /**
     * The initialization method for {@link Source}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     *
     * @param sourceEventListener After receiving events, the source should trigger onEvent() of this listener.
     *                            Listener will then pass on the events to the appropriate mappers for processing .
     * @param optionHolder        Option holder containing static configuration related to the {@link Source}
     * @param configReader        ConfigReader is used to read the {@link Source} related system configuration.
     * @param siddhiAppContext    the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to get Siddhi
     *                            related utility functions.
     */
    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                     String[] requestedTransportPropertyNames, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        consumerKey = optionHolder.validateAndGetStaticValue(TwitterConstants.CONSUMER_KEY);
        consumerSecret = optionHolder.validateAndGetStaticValue(TwitterConstants.CONSUMER_SECRET);
        accessToken = optionHolder.validateAndGetStaticValue(TwitterConstants.ACCESS_TOKEN);
        accessSecret = optionHolder.validateAndGetStaticValue(TwitterConstants.ACCESS_SECRET);
        mode = optionHolder.validateAndGetStaticValue(TwitterConstants.MODE);
        locationParam = optionHolder.validateAndGetStaticValue(TwitterConstants.STREAMING_FILTER_LOCATIONS,
                TwitterConstants.EMPTY_STRING);
        followParam = optionHolder.validateAndGetStaticValue(TwitterConstants.STREAMING_FILTER_FOLLOW,
                TwitterConstants.EMPTY_STRING);
        languageParam = optionHolder.validateAndGetStaticValue(TwitterConstants.STREAMING_FILTER_LANGUAGE,
                TwitterConstants.EMPTY_STRING);
        trackParam = optionHolder.validateAndGetStaticValue(TwitterConstants.STREAMING_FILTER_TRACK,
                TwitterConstants.EMPTY_STRING);
        filterLevel = optionHolder.validateAndGetStaticValue(TwitterConstants.STREAMING_FILTER_FILTER_LEVEL,
                "none");
        queryParam = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_QUERY,
                TwitterConstants.EMPTY_STRING);
        count = Integer.parseInt(optionHolder.validateAndGetStaticValue(
                TwitterConstants.POLLING_SEARCH_COUNT, "-1"));
        geocode = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_GEOCODE,
                TwitterConstants.EMPTY_STRING);
        maxId = Long.parseLong(optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_MAXID,
                "-1"));
        sinceId = Long.parseLong(optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_SINCEID,
                "-1"));
        searchLang = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_LANGUAGE,
                TwitterConstants.EMPTY_STRING);
        until = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_UNTIL,
                TwitterConstants.EMPTY_STRING);
        since = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_SINCE,
                TwitterConstants.EMPTY_STRING);
        resultType = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_RESULT_TYPE,
                "mixed");
        pollingInterval = Long.parseLong(optionHolder.validateAndGetStaticValue
                (TwitterConstants.POLLING_INTERVAL, "3600"));
        staticOptionsKeys = optionHolder.getStaticOptionsKeys();
        scheduledExecutorService = siddhiAppContext.getScheduledExecutorService();
        validateParameter();
    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{Map.class};
    }

    /**
     * Initially Called to connect to the end point for start retrieving the messages asynchronously .
     *
     * @param connectionCallback Callback to pass the ConnectionUnavailableException in case of connection failure after
     *                           initial successful connection. (can be used when events are receiving asynchronously)
     * @throws ConnectionUnavailableException if it cannot connect to the source backend immediately.
     */
    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        Query query;
        FilterQuery filterQuery;
        Twitter twitter;
        try {
            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.setOAuthConsumerKey(consumerKey)
                    .setOAuthConsumerSecret(consumerSecret)
                    .setOAuthAccessToken(accessToken)
                    .setOAuthAccessTokenSecret(accessSecret)
                    .setJSONStoreEnabled(true);
            if (mode.equalsIgnoreCase(TwitterConstants.MODE_STREAMING)) {
                twitterStream = (new TwitterStreamFactory(configurationBuilder.build())).getInstance();
                filterQuery = QueryBuilder.createFilterQuery(languageParam, trackParam, follow, filterLevel,
                        locations);
                twitterStatusListener = new TwitterStatusListener(sourceEventListener);
                twitterStream.addListener(twitterStatusListener);
                if (staticOptionsKeys.size() == TwitterConstants.MANDATORY_PARAM_SIZE) {
                    twitterStream.sample();
                } else {
                    twitterStream.filter(filterQuery);
                }
            } else if (mode.equalsIgnoreCase(TwitterConstants.MODE_POLLING)) {
                twitter = (new TwitterFactory(configurationBuilder.build())).getInstance();
                query = QueryBuilder.createQuery(queryParam, count, searchLang, sinceId, maxId, until, since,
                        resultType, geocode, latitude, longitude, radius, unitName);
                twitterPoller = new TwitterPoller(twitter, query, sourceEventListener);
                scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(
                        twitterPoller, 0, pollingInterval,
                        TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new ConnectionUnavailableException(
                    "Error in connecting with the Twitter API : " + e.getMessage(), e);
        }
    }

    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override
    public void disconnect() {
        if (twitterStream != null) {
            twitterStream.clearListeners();
            if (log.isDebugEnabled()) {
                log.debug("The status listener has been cleared!");
            }
        }
    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}
     */
    @Override
    public void destroy() {
        if (twitterStream != null) {
            twitterStream.shutdown();
            if (log.isDebugEnabled()) {
                log.debug("The twitter stream has been shutdown !");
            }
        }
        if (mode.equalsIgnoreCase(TwitterConstants.MODE_POLLING)) {
            scheduledFuture.cancel(true);
            twitterPoller.kill();
        }
    }

    /**
     * Called to pause event consumption
     */
    @Override
    public void pause() {
        if (mode.equalsIgnoreCase(TwitterConstants.MODE_STREAMING)) {
            twitterStatusListener.pause();
        } else {
            twitterPoller.pause();
        }
    }

    /**
     * Called to resume event consumption
     */
    @Override
    public void resume() {
        if (mode.equalsIgnoreCase(TwitterConstants.MODE_STREAMING)) {
            twitterStatusListener.resume();
        } else {
            twitterPoller.resume();
        }
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as a map
     */
    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();
        currentState.put(TwitterConstants.POLLING_SEARCH_SINCEID, twitterPoller.tweetId);
        return currentState;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param map the stateful objects of the processing element as a map.
     *            This map will have the  same keys that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> map) {
        sinceId = Long.parseLong(map.get(TwitterConstants.POLLING_SEARCH_SINCEID).toString());
    }

    /**
     * Used to validate the parameters.
     */

    private void validateParameter() {
        Query.ResultType resultType1 = Query.ResultType.valueOf(this.resultType);
        if (mode.equalsIgnoreCase(TwitterConstants.MODE_STREAMING)) {
            for (String staticOptionKey : staticOptionsKeys) {
                if (!TwitterConstants.STREAMING_PARAM.contains(staticOptionKey) &&
                        !TwitterConstants.MANDATORY_PARAM.contains(staticOptionKey)) {
                    throw new SiddhiAppValidationException(staticOptionKey + " is not valid for the " + mode + " " +
                            TwitterConstants.MODE);
                }
            }
        } else if (mode.equalsIgnoreCase(TwitterConstants.MODE_POLLING)) {
            if (queryParam.isEmpty()) {
                throw new SiddhiAppValidationException("For polling mode, query should be given.");
            }
            for (String staticOptionkey : staticOptionsKeys) {
                if (!TwitterConstants.POLLING_PARAM.contains(staticOptionkey) &&
                        !TwitterConstants.MANDATORY_PARAM.contains(staticOptionkey)) {
                    throw new SiddhiAppValidationException(staticOptionkey + " is not valid for the " + mode + " " +
                            TwitterConstants.MODE);
                }
            }
        } else {
            throw new SiddhiAppValidationException("There are only two possible values for mode :" +
                    " streaming or polling. But found '" + mode + "'.");
        }
        if (!followParam.isEmpty()) {
            follow = Util.followParam(followParam);
        }

        if (!locationParam.isEmpty()) {
            locations = Util.locationParam(locationParam);
        }

        if (!geocode.isEmpty()) {
            Query.Unit unit = null;
            String[] parts = geocode.split(TwitterConstants.DELIMITER);
            String radiusstr = parts[2].trim();
            try {
                latitude = Double.parseDouble(parts[0]);
                longitude = Double.parseDouble(parts[1]);
                radius = Double.parseDouble(radiusstr.substring(0, radiusstr.length() - 2));
            } catch (NumberFormatException e) {
                throw new SiddhiAppValidationException("In geocode, Latitude,Longitude,Radius should be " +
                        "a double value : " + e.getMessage());
            }

            for (Query.Unit value : Query.Unit.values()) {
                if (radiusstr.endsWith(value.name())) {
                    unit = value;
                    break;
                }
            }

            if (unit == null) {
                throw new SiddhiAppValidationException("Unrecognized geocode radius: " + radiusstr + ". Radius units" +
                        " must be specified as either 'mi' (miles) or 'km' (kilometers).");
            }
            unitName = unit.name();
        }

        if (!TwitterConstants.FILTER_LEVELS.contains(filterLevel)) {
            throw new SiddhiAppValidationException("There are only three possible values for filter.level :" +
                    " low or medium or none. But found '" + filterLevel + "'.");
        }

        if (!TwitterConstants.RESULT_TYPES.contains(resultType1)) {
            throw new SiddhiAppValidationException("There are only three possible values for result.type :" +
                    " mixed or popular or recent. But found '" + resultType + "'.");
        }
    }
}
