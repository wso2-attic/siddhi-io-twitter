# API Docs - v1.0.0-SNAPSHOT

## Sink

### twitter *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>*

<p style="word-wrap: break-word"> </p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@sink(type="twitter", @map(...)))
```

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
 
```
<p style="word-wrap: break-word"> </p>

## Source

### twitter *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*

<p style="word-wrap: break-word">The twitter source receives the events from an twitter API </p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(type="twitter", consumer.key="<STRING>", consumer.secret="<STRING>", access.token="<STRING>", access.token.secret="<STRING>", mode="<STRING>", filterlevel="<STRING>", track="<STRING>", follow="<LONG>", location="<DOUBLE>", language="<STRING>", query="<STRING>", geocode="<STRING>", result.type="<STRING>", max.id="<LONG>", since.id="<LONG>", until="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">consumer.key</td>
        <td style="vertical-align: top; word-wrap: break-word">Consumer key is the API key to access created twitter app</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">consumer.secret</td>
        <td style="vertical-align: top; word-wrap: break-word">Consumer secret is the API secret to access created twitter app</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">access.token</td>
        <td style="vertical-align: top; word-wrap: break-word">This access token is used to make API requests on behalf of your account.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">access.token.secret</td>
        <td style="vertical-align: top; word-wrap: break-word">This access token is used to make API requests on behalf of your account.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">mode</td>
        <td style="vertical-align: top; word-wrap: break-word">There are two possible values for mode. <br>1. Streaming - Retrieves real time tweets, <br>2. Polling - Retrieves historical tweets within one week).</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">filterlevel</td>
        <td style="vertical-align: top; word-wrap: break-word">Filters the tweets from the twitter stream based on the parameter filter_level. Values will be one of either none, low, or medium.</td>
        <td style="vertical-align: top">none</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">track</td>
        <td style="vertical-align: top; word-wrap: break-word">Filters the tweets that include the given keywords.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">follow</td>
        <td style="vertical-align: top; word-wrap: break-word">Filters the tweets that is tweeted by the given follow ids</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">LONG</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">location</td>
        <td style="vertical-align: top; word-wrap: break-word">Specifies the locations to track. Here, We have to specify latitude and the longitude of tha location. For Example : 51.683979:0.278970</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">language</td>
        <td style="vertical-align: top; word-wrap: break-word">Restricts tweets to the given language, given by an ISO 639-1 code.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">query</td>
        <td style="vertical-align: top; word-wrap: break-word">Query is a UTF-8, URL-encoded search query of 500 characters maximum, including operators. <br>For example : '@NASA - mentioning Twitter account 'NASA'.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">geocode</td>
        <td style="vertical-align: top; word-wrap: break-word">Returns tweets by users located within a given radius of the given latitude/longitude. The location is preferentially taking from the Geotagging API, but will fall back to their Twitter profile. The parameter value is specified by latitude,longitude,radius, where radius units must be specified as either ” mi ” (miles) or ” km ” (kilometers). Note that you cannot use the near operator via the API to geocode arbitrary locations; however you can use this geocode parameter to search near geocodes directly. A maximum of 1,000 distinct “sub-regions” will be considered when using the radius modifier</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">result.type</td>
        <td style="vertical-align: top; word-wrap: break-word">Specifies what type of results you would prefer to receive. The current default is 'mixed'. Valid values include:<br>* mixed : Include both popular and real time results in the response.<br>* recent : return only the most recent results in the response<br>* popular : return only the most popular results in the response.)</td>
        <td style="vertical-align: top">mixed</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">max.id</td>
        <td style="vertical-align: top; word-wrap: break-word">Returns results with an ID less than (that is, older than) or equal to the specified ID</td>
        <td style="vertical-align: top">-1</td>
        <td style="vertical-align: top">LONG</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">since.id</td>
        <td style="vertical-align: top; word-wrap: break-word">Returns results with an ID greater than (that is, more recent than) the specified ID. There are limits to the number of Tweets which can be accessed through the API. If the limit of Tweets has occurred since the since_id, the since_id will be forced to the oldest ID available</td>
        <td style="vertical-align: top">-1</td>
        <td style="vertical-align: top">LONG</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">until</td>
        <td style="vertical-align: top; word-wrap: break-word">Returns tweets created before the given date. Date should be formatted as YYYY-MM-DD. Search index has a 7-day limit. So no tweets will be found for a date older than one week.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type='twitter', consumer.key='consumer.key',consumer.secret='consumerSecret', access.token='accessToken',access.token.secret='accessTokenSecret', mode= 'streaming', language = 'en', @map(type='json', fail.on.missing.attribute='false' , attributes(created_at = 'created_at', id = 'id' ,id_str = 'id_str', text = 'text')))
define stream rcvEvents(created_at String, id long, id_str String, text String);
```
<p style="word-wrap: break-word">Under this configuration, it starts listening on random sample of public statuses and they are passed to the rcvEvents stream.</p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@source(type='twitter', consumer.key='consumer.key',consumer.secret='consumerSecret', access.token='accessToken',access.token.secret='accessTokenSecret', mode= 'streaming', track = 'Amazon,Google,Apple', language = 'en', @map(type='json', fail.on.missing.attribute='false' , attributes(created_at = 'created_at', id = 'id' ,id_str = 'id_str', text = 'text')))
define stream rcvEvents(created_at String, id long, id_str String, text String);
```
<p style="word-wrap: break-word">Under this configuration, it starts listening tweets in English that containing the keywords Amazon,google or apple and they are passed to the rcvEvents stream.</p>

<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@source(type='twitter', consumer.key='consumer.key',consumer.secret='consumerSecret', access.token='accessToken',access.token.secret='accessTokenSecret', mode= 'polling', query = 'happy hour', @map(type='json', fail.on.missing.attribute='false' , attributes(created_at = 'created_at', id = 'id' ,id_str = 'id_str', text = 'text')))
define stream rcvEvents(created_at String, id long, id_str String, text String);
```
<p style="word-wrap: break-word">Under this configuration, it starts listening tweets containing the exact phrase 'happy hour' and they are passed to the rcvEvents stream.</p>

