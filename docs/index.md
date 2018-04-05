Siddhi-io-twitter
======================================

The **siddhi-io-twitter extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> which is used to publish event data from Twitter App. It supports key-value map format.

## Prerequisites

* Go to the https://apps.twitter.com/ and create new App
* Select the app created in step 1 and go to"permission" tab and select "read&write" permission.
* Go to the "keys and access tokens" tab and generate new access token
* Collect following value from "keys and access tokens tab"
  * Consumer key
  * Consumer Secret
  * Access Token
  * Access Token Secret
* Update the parameter values for the extension with these values.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-twitter">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-twitter/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-twitter/issues">Issue tracker</a>

## Latest API Docs

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-twitter/api/1.0.3">1.0.3</a>.

## How to use

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support.

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github
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

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-twitter/api/1.0.3/#twitter-source">twitter</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>)*<br><div style="padding-left: 1em;"><p>The twitter source receives the events from a twitter App. </p></div>

## How to Contribute

  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-twitter/issues">GitHub Issue Tracker</a>.

  * Send your contributions as pull requests to <a target="_blank" href="https://github
  .com/wso2-extensions/siddhi-io-twitter/tree/master">master branch</a>.

## Contact us

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>.

 * Siddhi developers can be contacted via the mailing lists:

    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)

    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)

## Support

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology.

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.
