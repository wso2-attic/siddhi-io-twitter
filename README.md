Siddhi-io-twitter
======================================

The **siddhi-io-twitter extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a>. It publishes event data from Twitter Applications in the key-value map format.

## Prerequisites

* Create a new Twitter application in the <a target="_blank" href="https://apps.twitter.com/">Twitter Application Managemet page</a>.
* Open the application you created in step 1 and click on the **Permissions** tab. In this tab, select the **Read and Write** option.
* Click on the **Keys and Access Tokens** tab. Generate new access token by clicking **Create My Access Token**.
* Collect following values from the **Keys and Access Tokens** tab.
  * Consumer key
  * Consumer Secret
  * Access Token
  * Access Token Secret
* Update the parameter values for the extension with the values you collected in the previous step.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-twitter">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-twitter/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-twitter/issues">Issue tracker</a>

## Latest API Docs

Latest API ducumentation is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-twitter/api/1.0.6">1.0.6</a>.

## How to use

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension with the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of the <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support.

* This extension is shipped by default with WSO2 Stream Processor. If you need to use an alternative version of this extension, you can replace the component <a target="_blank" href="https://github
.com/wso2-extensions/siddhi-io-twitter/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.io.twitter</groupId>
        <artifactId>siddhi-io-twitter</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ |
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-twitter/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-twitter/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-twitter/api/1.0.6/#twitter-source">twitter</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>)*<br><div style="padding-left: 1em;"><p>The Twitter source receives the events from a Twitter application. The events are received in a key-value map. <br><br> The following are the key values in the map of a Tweet and their descriptions.<br>&nbsp;&nbsp;&nbsp;&nbsp;1.  createdAt - UTC time when this Tweet was created.<br>&nbsp;&nbsp;&nbsp;&nbsp;2.  tweetId - The integer representation of the unique identifier for this Tweet.<br>&nbsp;&nbsp;&nbsp;&nbsp;3.  text - The actual UTF-8 text of the status update.<br>&nbsp;&nbsp;&nbsp;&nbsp;4.  user.createdAt - The UTC datetime that the user account was created on Twitter.<br>&nbsp;&nbsp;&nbsp;&nbsp;5.  user.id - The integer representation of the unique identifier for this User.<br>&nbsp;&nbsp;&nbsp;&nbsp;6.  user.screenName - The screen name, that this user identifies themselves with.<br>&nbsp;&nbsp;&nbsp;&nbsp;7.  user.name - The name of the user, as they've defined it.<br>&nbsp;&nbsp;&nbsp;&nbsp;8.  user.mail - The mail.id of the user.<br>&nbsp;&nbsp;&nbsp;&nbsp;9.  user.location - Nullable. The user-defined location for this account's profile.<br>&nbsp;&nbsp;&nbsp;&nbsp;10. hashtags - Represents hashtags which have been parsed out of the Tweet.<br>&nbsp;&nbsp;&nbsp;&nbsp;11. userMentions - Represents other Twitter users mentioned in the text of the Tweet.<br>&nbsp;&nbsp;&nbsp;&nbsp;12. mediaUrls - Represents media elements uploaded with the Tweet.<br>&nbsp;&nbsp;&nbsp;&nbsp;13. urls - Represents URLs included in the text of a Tweet.<br>&nbsp;&nbsp;&nbsp;&nbsp;14. language - The language inwhich tweep tweeted.<br>&nbsp;&nbsp;&nbsp;&nbsp;15. source - Utility used to post the Tweet, as an HTML-formatted string<br>&nbsp;&nbsp;&nbsp;&nbsp;16. isRetweet - Indicates whether this is a Retweeted Tweet.<br>&nbsp;&nbsp;&nbsp;&nbsp;17. retweetCount - Number of times this Tweet has been retweeted.<br>&nbsp;&nbsp;&nbsp;&nbsp;18. favouriteCount = Nullable. Indicates approximately how many times this Tweet has been liked by Twitter users.<br>&nbsp;&nbsp;&nbsp;&nbsp;19. geoLocation - Nullable. Represents the geographic location of this Tweet as reported by the user or client application.<br>&nbsp;&nbsp;&nbsp;&nbsp;20. quotedStatusId - This field only surfaces when the Tweet is a quote Tweet. This field contains the integer value Tweet ID of the quoted Tweet.<br>&nbsp;&nbsp;&nbsp;&nbsp;21. in.reply.to.status.id - Nullable. If the represented Tweet is a reply, this field will contain the integer representation of the original Tweet's ID.<br>&nbsp;&nbsp;&nbsp;&nbsp;22. place.id - ID representing this place. This is represented as a string, not an integer.<br>&nbsp;&nbsp;&nbsp;&nbsp;23. place.name - Short human-readable representation of the place's name.<br>&nbsp;&nbsp;&nbsp;&nbsp;24. place.fullName - Full human-readable representation of the place's name.<br>&nbsp;&nbsp;&nbsp;&nbsp;25. place.country_code - Shortened country code representing the country containing this place.<br>&nbsp;&nbsp;&nbsp;&nbsp;26. place.country - Name of the country containing this place.<br>&nbsp;&nbsp;&nbsp;&nbsp;27. track.words - Keywords given by the user to track.<br>&nbsp;&nbsp;&nbsp;&nbsp;28. polling.query - Query given by the user.<br>&nbsp;&nbsp;&nbsp;&nbsp;</p></div>

## How to Contribute

  * Report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-twitter/issues">GitHub Issue Tracker</a>.

  * Send your contributions as pull requests to the <a target="_blank" href="https://github
  .com/wso2-extensions/siddhi-io-twitter/tree/master">master branch</a>.

## Contact us

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>.

 * Siddhi developers can be contacted via the following mailing lists:

    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)

    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)

## Support

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology.

* For more details and to take advantage of this unique opportunity contact us via<a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.
