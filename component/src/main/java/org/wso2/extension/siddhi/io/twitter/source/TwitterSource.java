package org.wso2.extension.siddhi.io.twitter.source;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.twitter.util.TwitterConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
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
                        description = "This is a mandatory parameter. Consumer key is one of the " +
                                "user credentials to access twitter API.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "consumer.secret",
                        description = "This is a mandatory parameter. Consumer Secret is one of the " +
                                "user credentials to access twitter API.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "access.token",
                        description = "This is a mandatory parameter. Access Token is one of the " +
                                "user credentials to access twitter API.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "access.token.secret",
                        description = "This is a mandatory parameter. Access Token Secret is one of the " +
                                "user credentials " +
                                "to access twitter API.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "mode",
                        description = "This is also a mandatory parameter. There are two types of " +
                                "modes(Streaming, Polling).",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "filterlevel",
                        description = "This is a optional parameter. The filter level limits " +
                                "what tweets appear in the stream to those with a minimum filter_level" +
                                " attribute value. Values will be one of either none, low, or medium.",
                        optional = true,
                        defaultValue = "none",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "track",
                        description = "This is a optional parameter which filters the tweets that" +
                                " include the given keywords.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "follow",
                        description = "This is a optional parameter which specifies the users," +
                                " by ID, to receive public tweets from.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.LONG}),
                @Parameter(
                        name = "location",
                        description = "This is a optional parameters which specifies the locations to track." +
                                " Here, We have to specify latitude and the longitude of tha location",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.DOUBLE}),
                @Parameter(
                        name = "query",
                        description = "This is a optional parameter which is a UTF-8, " +
                                "URL-encoded search query of 500 characters maximum, including operators.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "geocode",
                        description = "This is a optional parameter which returns tweets by users located " +
                                "within a given radius of the given latitude/longitude. The location is " +
                                "preferentially taking from the Geotagging API, but will fall back to their " +
                                "Twitter profile. The parameter value is specified by " +
                                "latitude,longitude,radius, where radius units must be specified as " +
                                "either ” mi ” (miles) or ” km ” (kilometers). Note that you cannot use the " +
                                "near operator via the API to geocode arbitrary locations; however " +
                                "you can use this geocode parameter to search near geocodes directly. " +
                                "A maximum of 1,000 distinct “sub-regions” will be considered when " +
                                "using the radius modifier",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "result.type",
                        description = "This is a optional parameter which specifies what type" +
                                " results you would prefer to receive. The current default is “mixed.” " +
                                "Valid values include:\n" +
                                "* mixed : Include both popular and real time results in the response.\n" +
                                "* recent : return only the most recent results in the response\n" +
                                "* popular : return only the most popular results in the response.)",
                        optional = true,
                        defaultValue = "mixed",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "language",
                        description = "This is a optional parameter which restricts tweets to the " +
                                "given language, given by an ISO 639-1 code.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "max.id",
                        description = "This is a optional parameter which returns results with an ID " +
                                "less than (that is, older than) or equal to the specified ID",
                        optional = true,
                        defaultValue = "0",
                        type = {DataType.LONG}),
                @Parameter(
                        name = "since.id",
                        description = "This is a optional parameter which returns results with an ID greater than " +
                                "(that is, more recent than) the specified ID. " +
                                "There are limits to the number of Tweets which can be accessed through" +
                                " the API. If the limit of Tweets" +
                                " has occurred since the since_id, the since_id will be forced to the " +
                                "oldest ID available",
                        optional = true,
                        defaultValue = "0",
                        type = {DataType.LONG}),
                @Parameter(
                        name = "until",
                        description = "This is a optional parameter which returns tweets created" +
                                " before the given date." +
                                " Date should be formatted as YYYY-MM-DD. \n" +
                                " Search index has a 7-day limit. So no tweets will be found for a " +
                                "date older than one week.",
                        optional = true,
                        defaultValue = "null",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@source(type='twitter', consumer.key='consumer.key'," +
                                "consumer.secret='consumerSecret', access.token='accessToken'," +
                                "access.token.secret='accessTokenSecret', mode= 'streaming'" +
                                ", track = 'Amazon,Google,Apple', language = 'en', @map(type='json', " +
                                "fail.on.missing.attribute='false' , attributes(created_at = 'created_at'," +
                                " id = 'id' ,id_str = 'id_str', text = 'text')))\n" +
                                "define stream rcvEvents(created_at String, id long, id_str String, text String);",
                        description = "Under this configuration, it starts listening on random " +
                                "sample of all public statuses and they are passed to the rcvEvents stream."
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
    /*private ArrayList<String> filterlevels;
    private ArrayList<String> resultTypes;*/
    private String[] filterlevels;
    private String[] resultTypes;
    private TwitterStream twitterStream;
    private String siddhiAppName;
    private Twitter twitter;

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
        this.siddhiAppName = siddhiAppContext.getName();
        /*this.filterlevels = new ArrayList<String>() {
            {
                add("none");
                add("medium");
                add("low");
            }
        };
        this.resultTypes = new ArrayList<String>() {
            {
                add("mixed");
                add("popular");
                add("recent");
            }
        };*/
        this.filterlevels = new String[]{"none", "low", "medium"};
        this.resultTypes = new String[]{"mixed" , "popular", "recent"};
        this.consumerKey = optionHolder.validateAndGetStaticValue(TwitterConstants.CONSUMER_KEY);
        this.consumerSecret = optionHolder.validateAndGetStaticValue(TwitterConstants.CONSUMER_SECRET);
        this.accessToken = optionHolder.validateAndGetStaticValue(TwitterConstants.ACCESS_TOKEN);
        this.accessSecret = optionHolder.validateAndGetStaticValue(TwitterConstants.ACCESS_SECRET);
        this.mode = optionHolder.validateAndGetStaticValue(TwitterConstants.MODE);
        if (this.mode.equals("streaming") || this.mode.equals("polling")) {
            if (this.mode.equals("streaming")) {
                log.info("In Streaming mode, you can only give these following parameters. " +
                        "If you give any other parameters, they will be ignored.\n" +
                        "{track, language, follow, location, filterlevel}");

            } else {
                log.info("In polling mode, you can only give these following parameters. " +
                        "If you give any other parameters, they will be ignored.\n" +
                        "{query, geocode, max.id, since.id, language, result.type, until}");
            }
        } else {
            throw new SiddhiAppCreationException("There are only two possible values : streaming or polling");
        }

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
        /*if (!this.filterlevels.contains(filterLevel)) {
            throw new SiddhiAppCreationException("There are only three possible values : low or medium or none");
        }*/
        for (String filterlevel : this.filterlevels) {
            if (filterlevel.equals(filterLevel)) {
                break;
            } else {
                throw new SiddhiAppCreationException("There are only three possible values : none or " +
                        "low or medium");
            }
        }
        this.query = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_QUERY,
                TwitterConstants.EMPTY_STRING);
        this.geocode = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_GEOCODE,
                TwitterConstants.EMPTY_STRING);
        this.maxId = Long.parseLong(optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_MAXID,
                "0"));
        this.sinceId = Long.parseLong(optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_SINCEID,
                "0"));
        this.searchLang = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_LANGUAGE,
                TwitterConstants.EMPTY_STRING);
        this.until = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_UNTIL,
                TwitterConstants.EMPTY_STRING);
        this.resultType = optionHolder.validateAndGetStaticValue(TwitterConstants.POLLING_SEARCH_RESULT_TYPE,
                "mixed");
        for (String resulttype : this.resultTypes) {
            if (resulttype.equals(resultType)) {
                break;
            } else {
                throw new SiddhiAppCreationException("There are only three possible values : mixed or " +
                        "popular or recent");
            }
        }
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
        if (log.isDebugEnabled()) {
            log.debug("Starting to load the twitter credentials");
        }
        try {
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey(consumerKey)
                    .setOAuthConsumerSecret(consumerSecret)
                    .setOAuthAccessToken(accessToken)
                    .setOAuthAccessTokenSecret(accessSecret)
                    .setJSONStoreEnabled(true);
            if (this.mode.equals("streaming")) {
                this.twitterStream = (new TwitterStreamFactory(cb.build())).getInstance();
                TwitterConsumer.consume(this.twitterStream, this.sourceEventListener, this.languageParam,
                        this.trackParam, this.followParam, this.filterLevel, this.locationParam);
            }

            if (this.mode.equals("polling")) {
                this.twitter = (new TwitterFactory(cb.build())).getInstance();
                TwitterConsumer.consume(this.twitter, this.sourceEventListener, this.query,
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
        try {
            if (this.twitterStream != null) {
                this.twitterStream.clearListeners();
                if (log.isDebugEnabled()) {
                    log.debug("The status listener has been cleared!");
                }
            }
        } catch (Exception var2) {
            log.error("Error while clearing listeners" + var2.getMessage(), var2);
        }
    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}
     */
    @Override
    public void destroy() {
        try {
            if (this.twitterStream != null) {
                this.twitterStream.shutdown();
                if (log.isDebugEnabled()) {
                    log.debug("The twitter stream has been shutdown !");
                }
            }
        } catch (Exception var2) {
            log.error("Error while shutdown the twitter stream" + var2.getMessage(), var2);
        }
    }

    /**
     * Called to pause event consumption
     */
    @Override
    public void pause() {
    }

    /**
     * Called to resume event consumption
     */
    @Override
    public void resume() {
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

