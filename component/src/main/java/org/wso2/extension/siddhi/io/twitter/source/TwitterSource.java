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

/**
 * Twitter Source Implementation
 */
@Extension(
        name = "twitter",
        namespace = "source",
        description = "The twitter source receives the events from a twitter App. The events will be received " +
                "in a key-value map. \n\n" +
                "Key values of the map of a tweet and their descriptions.\n\t" +
                "1.  createdAt - UTC time when this Tweet was created.\n\t" +
                "2.  tweetId - The integer representation of the unique identifier for this Tweet.\n\t" +
                "3.  text - The actual UTF-8 text of the status update.\n\t" +
                "4.  user.createdAt - The UTC datetime that the user account was created on Twitter.\n\t" +
                "5.  user.id - The integer representation of the unique identifier for this User.\n\t" +
                "6.  user.screenName - The screen name, that this user identifies themselves with.\n\t" +
                "7.  user.name - The name of the user, as they've defined it.\n\t" +
                "8.  user.mail - The mail.id of the user.\n\t" +
                "9.  user.location - Nullable. The user-defined location for this account's profile.\n\t" +
                "10. hashtags - Represents hashtags which have been parsed out of the Tweet.\n\t" +
                "11. userMentions - Represents other Twitter users mentioned in the text of the Tweet.\n\t" +
                "12. mediaUrls - Represents media elements uploaded with the Tweet.\n\t" +
                "13. urls - Represents URLs included in the text of a Tweet.\n\t" +
                "14. language - The language inwhich tweep tweeted.\n\t" +
                "15. source - Utility used to post the Tweet, as an HTML-formatted string\n\t" +
                "16. isRetweet - Indicates whether this is a Retweeted Tweet.\n\t" +
                "17. retweetCount - Number of times this Tweet has been retweeted.\n\t" +
                "18. favouriteCount = Nullable. Indicates approximately how many times this Tweet has been liked" +
                " by Twitter users.\n\t" +
                "19. geoLocation - Nullable. Represents the geographic location of this Tweet as reported by the" +
                " user or client application.\n\t" +
                "20. quotedStatusId - This field only surfaces when the Tweet is a quote Tweet. This field contains " +
                "the integer value Tweet ID of the quoted Tweet.\n\t" +
                "21. in.reply.to.status.id - Nullable. If the represented Tweet is a reply, this field will contain" +
                " the integer representation of the original Tweet's ID.\n\t" +
                "22. place.id - ID representing this place. This is represented as a string, not an integer.\n\t" +
                "23. place.name - Short human-readable representation of the place's name.\n\t" +
                "24. place.fullName - Full human-readable representation of the place's name.\n\t" +
                "25. place.country_code - Shortened country code representing the country containing this place.\n\t" +
                "26. place.country - Name of the country containing this place.\n\t" ,
        parameters = {
                @Parameter(
                        name = "consumer.key",
                        description = "Consumer key is the API key to access created twitter app",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "consumer.secret",
                        description = "Consumer secret is the API secret to access created twitter app",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "access.token",
                        description = "Access token is used to make API requests on behalf" +
                                " of your account.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "access.token.secret",
                        description = "Access token secret is used to make API requests on behalf" +
                                " of your account.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "mode",
                        description = "There are two possible values for mode. \n" +
                                "1. streaming - Retrieves real time tweets, \n2. polling - Retrieves historical" +
                                " tweets within one week.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "filter.level",
                        description = "Filters tweets by the level of engagement based on the " +
                                " filter.level. The highest level(medium) corresponds loosely to the 'top tweets'" +
                                "filter the service already offers in its on-site search function. Values will " +
                                "be one of either none, low, or medium.",
                        optional = true,
                        defaultValue = "none",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "track",
                        description = "Filters the tweets that include the given keywords.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "follow",
                        description = "Filters the tweets that is tweeted by the given user ids",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.LONG}),
                @Parameter(
                        name = "location",
                        description = "Filters tweets based on the locations. Here, We have to specify " +
                                "latitude and the longitude of the location. For Example : 51.683979:0.278970",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.DOUBLE}),
                @Parameter(
                        name = "language",
                        description = "Filters tweets in the given language, given by an ISO 639-1 code.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "query",
                        description = "Filters tweets that matches the given Query, UTF-8, URL-encoded search" +
                                " query of 500 characters maximum, including operators. \nFor example : " +
                                "'@NASA' - mentioning Twitter account 'NASA'.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "count",
                        description = "Returns specified number of tweets per page, up to a maximum of 100.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "geocode",
                        description = "Returns tweets by users located within a given radius of the given " +
                                "latitude/longitude. The location is preferentially taking from the Geotagging" +
                                " API, but will fall back to their Twitter profile. The parameter value is specified" +
                                " by latitude,longitude,radius, where radius units must be specified as " +
                                "either 'mi' (miles) or 'km' (kilometers).",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "result.type",
                        description = "Returns tweets based on what type of results you would prefer to receive." +
                               "Valid values include:\n" +
                                "* mixed : Include both popular and recent results in the response.\n" +
                                "* recent : return only the most recent results in the response\n" +
                                "* popular : return only the most popular results in the response.)",
                        optional = true,
                        defaultValue = "mixed",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "max.id",
                        description = "Returns tweets with an tweet ID less than (that is, older than)" +
                                " or equal to the specified ID",
                        optional = true,
                        defaultValue = "-1",
                        type = {DataType.LONG}),
                @Parameter(
                        name = "since.id",
                        description = "Returns tweets with an tweet ID greater than (that is, more recent than)" +
                                " the specified ID.",
                        optional = true,
                        defaultValue = "-1",
                        type = {DataType.LONG}),
                @Parameter(
                        name = "until",
                        description = "Returns tweets created before the given date. Date should be" +
                                " formatted as YYYY-MM-DD. Search index has a 7-day limit. So no tweets" +
                                " will be found for a date older than one week.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "polling.interval",
                        description = "Specifies the period of time (in seconds) to poll tweets periodically",
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
                        description = "Under this configuration, it starts listening on random " +
                                "sample of public statuses and they are passed to the rcvEvents stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'streaming'" +
                                ", track = 'Amazon,Google,Apple', language = 'en', @map(type='keyvalue', " +
                                "@attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                                "hashtags = 'hashtags'))) \n" +
                                "define stream inputStream(createdAt String, id long, text String, hashtags string);",
                        description = "Under this configuration, it starts listening tweets in English that " +
                                "containing the keywords Amazon,google or apple and they are passed to the rcvEvents" +
                                " stream."
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
                        description = "Under this configuration, it starts listening tweets in English that " +
                                "containing the keywords Amazon,google,apple or tweeted by the given followers" +
                                " or tweeted from the given location based on the filter.level. and they are passed" +
                                " to the rcvEvents stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'polling'" +
                                ", query = 'happy hour', @map(type='keyvalue', @attributes(createdAt = 'createdAt'," +
                                " id = 'tweetId', text= 'text', hashtags = 'hashtags'))) \n" +
                                "define stream inputStream(createdAt String, id long, text String, hashtags string);",
                        description = "Under this configuration, it starts polling tweets containing the" +
                                " exact phrase 'happy hour' and they are passed to the rcvEvents stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'polling'" +
                                ", query = '#Amazon', since.id = '973439483906420736', @map(type='keyvalue', " +
                                "@attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                                "hashtags = 'hashtags'))) \n" +
                                "define stream inputStream(createdAt String, id long, text String, hashtags string);",
                        description = "Under this configuration, it starts polling tweets, containing the hashtag" +
                                " '#Amazon' and tweet Id is greater than since.id and they are passed to the " +
                                "rcvEvents stream."
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
                        description = "Under this configuration, it starts polling recent tweets in english that is " +
                                " having tweet id greater than since.id and less than max.id, mentioning NASA " +
                                " and they are passed to the rcvEvents stream."
                )
        }
)

public class TwitterSource extends Source {
    private static final Logger log = Logger.getLogger(TwitterSource.class);
    private TwitterConsumer twitterConsumer;
    private SourceEventListener sourceEventListener;
    private SiddhiAppContext siddhiAppContext;
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
        this.siddhiAppContext = siddhiAppContext;
        twitterConsumer = TwitterConsumer.INSTANCE;
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
                twitterConsumer.consume(twitterStream, sourceEventListener, filterQuery, staticOptionsKeys.size());
            } else {
                twitter = (new TwitterFactory(configurationBuilder.build())).getInstance();
                query = QueryBuilder.createQuery(queryParam, count, searchLang, sinceId, maxId, until, since,
                        resultType, geocode, latitude, longitude, radius, unitName);
                twitterConsumer.consume(twitter, query, sourceEventListener, siddhiAppContext, pollingInterval);
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
            twitterStream.shutdown();
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
            twitterStream.clearListeners();
            if (log.isDebugEnabled()) {
                log.debug("The twitter stream has been shutdown !");
            }
        }
    }

    /**
     * Called to pause event consumption
     */
    @Override
    public void pause() {
        twitterConsumer.pause();
    }

    /**
     * Called to resume event consumption
     */
    @Override
    public void resume() {
        twitterConsumer.resume();
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
        currentState.put(TwitterConstants.POLLING_SEARCH_SINCEID, twitterConsumer.tweetId);
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
            for (String parameters : staticOptionsKeys) {
                if (!TwitterConstants.POLLING_PARAM.contains(parameters) &&
                        !TwitterConstants.MANDATORY_PARAM.contains(parameters)) {
                    throw new SiddhiAppValidationException(parameters + " is not valid for the " + mode + " " +
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
