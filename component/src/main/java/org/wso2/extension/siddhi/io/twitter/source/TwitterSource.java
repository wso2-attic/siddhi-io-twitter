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
import org.wso2.extension.siddhi.io.twitter.util.TwitterConstants;
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
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.Set;

 /**
 * Twitter Source Implementation
 */

@Extension(
        name = "twitter",
        namespace = "source",
        description = "The twitter source receives the events from an twitter API ",
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
                                "1. Streaming - Retrieves real time tweets, \n2. Polling - Retrieves historical" +
                                " tweets within one week.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "filter.level",
                        description = "Filters tweets by the level of engagement based on the " +
                                " filter.level. The highest level(medium) corresponds loosely to the “top tweets” " +
                                "filter the service already offers in its on-site search function Values will be one " +
                                "of either none, low, or medium.",
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
                        description = "Filters the tweets based on the locations. Here, We have to specify " +
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
                        description = "Query is a UTF-8, URL-encoded search query of 500 characters maximum, " +
                                "including operators. \nFor example : '@NASA - mentioning Twitter account 'NASA'.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "geocode",
                        description = "Returns tweets by users located within a given radius of the given " +
                                "latitude/longitude. The location is preferentially taking from the Geotagging" +
                                " API, but will fall back to their Twitter profile. The parameter value is specified" +
                                " by latitude,longitude,radius, where radius units must be specified as " +
                                "either ” mi ” (miles) or ” km ” (kilometers).",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "result.type",
                        description = "Returns the tweets based on what type of results you would prefer to receive." +
                                " The current default is 'mixed'. Valid values include:\n" +
                                "* mixed : Include both popular and recent results in the response.\n" +
                                "* recent : return only the most recent results in the response\n" +
                                "* popular : return only the most popular results in the response.)",
                        optional = true,
                        defaultValue = "mixed",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "max.id",
                        description = "Returns results with an tweet ID less than (that is, older than)" +
                                " or equal to the specified ID",
                        optional = true,
                        defaultValue = "-1",
                        type = {DataType.LONG}),
                @Parameter(
                        name = "since.id",
                        description = "Returns results with an tweet ID greater than (that is, more recent than)" +
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
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'streaming', " +
                                "@map(type='json', fail.on.missing.attribute='false', attributes" +
                                "(created_at = 'created_at', id = 'id' ,id_str = 'id_str', " +
                                "text = 'text')))\n" +
                                "define stream rcvEvents(created_at String, id long, id_str String, text String);",
                        description = "Under this configuration, it starts listening on random " +
                                "sample of public statuses and they are passed to the rcvEvents stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'streaming'" +
                                ", track = 'Amazon,Google,Apple', language = 'en', @map(type='json', " +
                                "fail.on.missing.attribute='false' , attributes(created_at = 'created_at'," +
                                " id = 'id' ,id_str = 'id_str', text = 'text')))\n" +
                                "define stream rcvEvents(created_at String, id long, id_str String, text String);",
                        description = "Under this configuration, it starts listening tweets in English that " +
                                "containing the keywords Amazon,google or apple and they are passed to the rcvEvents" +
                                " stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'streaming'" +
                                ", track = 'Amazon,Google,Apple', language = 'en', follow = '11348282,20536157," +
                                "15670515,17193794,58561993,18139619',filter.level = 'low', " +
                                "location = '51.280430:-0.563160,51.683979:0.278970', @map(type='json', " +
                                "fail.on.missing.attribute='false' , attributes(created_at = 'created_at'," +
                                " id = 'id' ,id_str = 'id_str', text = 'text')))\n" +
                                "define stream rcvEvents(created_at String, id long, id_str String, text String);",
                        description = "Under this configuration, it starts listening tweets in English that " +
                                "containing the keywords Amazon,google or apple or tweeted by the given followers" +
                                " or tweeted from the given location based on the filter.level. and they are passed" +
                                " to the rcvEvents stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'polling'" +
                                ", query = 'happy hour', @map(type='json', " +
                                "fail.on.missing.attribute='false' , attributes(created_at = 'created_at'," +
                                " id = 'id' ,id_str = 'id_str', text = 'text')))\n" +
                                "define stream rcvEvents(created_at String, id long, id_str String, text String);",
                        description = "Under this configuration, it starts polling tweets containing the" +
                                " exact phrase 'happy hour' and they are passed to the rcvEvents stream."
                ),
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'polling'" +
                                ", query = '@NASA', language = 'en', result.type = 'recent'," +
                                " geocode = '43.913723261972855,-72.54272478125,150km', since.id = 24012619984051000," +
                                " max.id = 250126199840518145, until = 2018-03-10,  @map(type='json', " +
                                "fail.on.missing.attribute='false' , attributes(created_at = 'created_at'," +
                                " id = 'id' ,id_str = 'id_str', text = 'text')))\n" +
                                "define stream rcvEvents(created_at String, id long, id_str String, text String);",
                        description = "Under this configuration, it starts polling recent tweets in english that" +
                                " having tweet id greater than since.id and less than ma.id, mentioning NASA " +
                                " and they are passed to the rcvEvents stream."
                )
        }
)

public class TwitterSource extends Source {
    private static final Logger log = Logger.getLogger(TwitterSource.class);
    private SourceEventListener sourceEventListener;
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessSecret;
    private String mode;
    private String followParam;
    private String trackParam;
    private String languageParam;
    private String locationParam;
    private String filterLevel;
    private String query;
    private String geocode;
    private long maxId;
    private long sinceId;
    private String searchLang;
    private String until;
    private String resultType;
    private TwitterStream twitterStream;


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
        Set<String> staticOptionsKeys;
        this.sourceEventListener = sourceEventListener;
        this.consumerKey = optionHolder.validateAndGetStaticValue(TwitterConstants.CONSUMER_KEY);
        this.consumerSecret = optionHolder.validateAndGetStaticValue(TwitterConstants.CONSUMER_SECRET);
        this.accessToken = optionHolder.validateAndGetStaticValue(TwitterConstants.ACCESS_TOKEN);
        this.accessSecret = optionHolder.validateAndGetStaticValue(TwitterConstants.ACCESS_SECRET);
        this.mode = optionHolder.validateAndGetStaticValue(TwitterConstants.MODE);
        this.followParam = optionHolder.validateAndGetStaticValue(TwitterConstants.STREAMING_FILTER_FOLLOW,
                TwitterConstants.EMPTY_STRING);
        this.languageParam = optionHolder.validateAndGetStaticValue(TwitterConstants.STREAMING_FILTER_LANGUAGE,
                TwitterConstants.EMPTY_STRING);
        this.locationParam = optionHolder.validateAndGetStaticValue(TwitterConstants.STREAMING_FILTER_LOCATIONS,
                TwitterConstants.EMPTY_STRING);
        this.trackParam = optionHolder.validateAndGetStaticValue(TwitterConstants.STREAMING_FILTER_TRACK,
                TwitterConstants.EMPTY_STRING);
        this.filterLevel = optionHolder.validateAndGetStaticValue(TwitterConstants.STREAMING_FILTER_FILTER_LEVEL,
                "none");
        this.query = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_QUERY,
                TwitterConstants.EMPTY_STRING);
        this.geocode = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_GEOCODE,
                TwitterConstants.EMPTY_STRING);
        this.maxId = Long.parseLong(optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_MAXID,
                "-1"));
        this.sinceId = Long.parseLong(optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_SINCEID,
                "-1"));
        this.searchLang = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_LANGUAGE,
                TwitterConstants.EMPTY_STRING);
        this.until = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_UNTIL,
                TwitterConstants.EMPTY_STRING);
        this.resultType = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_RESULT_TYPE,
                "mixed");
        staticOptionsKeys = optionHolder.getStaticOptionsKeys();
        TwitterConsumer.validateParameter(mode, query, filterLevel, resultType, staticOptionsKeys);

    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[0];
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
        Twitter twitter;
        try {
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey(consumerKey)
                    .setOAuthConsumerSecret(consumerSecret)
                    .setOAuthAccessToken(accessToken)
                    .setOAuthAccessTokenSecret(accessSecret)
                    .setJSONStoreEnabled(true)
                    .setIncludeEntitiesEnabled(true);
            if (this.mode.equalsIgnoreCase(TwitterConstants.MODE_STREAMING)) {
                this.twitterStream = (new TwitterStreamFactory(cb.build())).getInstance();
                TwitterConsumer.consume(this.twitterStream, this.sourceEventListener, this.languageParam,
                        this.trackParam, this.followParam, this.filterLevel, this.locationParam);
            }

            if (this.mode.equalsIgnoreCase(TwitterConstants.MODE_POLLING)) {
                twitter = (new TwitterFactory(cb.build())).getInstance();
                TwitterConsumer.consume(twitter, this.sourceEventListener, this.query,
                        this.searchLang, this.sinceId, this.maxId, this.until, this.resultType, this.geocode);
            }
        } catch (Exception e) {
            throw new ConnectionUnavailableException(
                    "Error in connecting with the Twitter API" + e.getMessage(), e);
        }
    }


    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override
    public void disconnect() {
        if (this.twitterStream != null) {
            this.twitterStream.shutdown();
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
        if (this.twitterStream != null) {
            this.twitterStream.clearListeners();
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
        TwitterConsumer.pause();
    }

    /**
     * Called to resume event consumption
     */
    @Override
    public void resume() {
        TwitterConsumer.resume();
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as a map
     */
    @Override
    public Map<String, Object> currentState() {
        return null;
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
    }
}

