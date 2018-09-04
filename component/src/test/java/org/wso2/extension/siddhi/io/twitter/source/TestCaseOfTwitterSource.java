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
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfTwitterSource {
    private static final Logger LOG = Logger.getLogger(TestCaseOfTwitterSource.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private AtomicBoolean eventArrived = new AtomicBoolean(false);
    private int waitTime = 50;
    private int timeout = 10000;
    private String streamingConsumerKey = "4qeo5GROPuSjJwIjA59eMjMfu";
    private String streamingConsumerSecret = "5e4yM0QT653Dabsy6dVj8r4zOvNjBOKrPfV2huIBNn5HTTRZAd";
    private String streamingAccessToken = "948469744398733312-m2Qv52gCiyM3Rc1uKa5qIWlLX1ehpOm";
    private String streamingAccessTokenSecret = "Cqzh7UKlbk0s6597fwLFwRshMV2NOEm3bLyKD6vp6N1c0";
    private String consumerKey = "5dU1zRlWDbRRl5BAeiBMcjg9L";
    private String consumerSecret = "ZcoN9inlymuIeVklAtFT8oY68BNd8PmHZaItZOKs7F4xxcys9O";
    private String accessToken = "345216227-bZDqBywsRupsc7iZZo82eO01EHbZJp8C5BSbKSl7";
    private String accessTokenSecret = "jYovH0mm3vLehpkYFdYoIwRdRiZin5dtgqDyHENRn8ZEm";

    @BeforeMethod
    public void init() {
        eventCount.set(0);
        eventArrived.set(false);
    }

    @Test
    public void testTwitterStreaming1() throws InterruptedException {
        LOG.info("------------------------------------------------------------------------------------------------");
        LOG.info("TwitterSourceStreaming TestCase 1 - Filtering tweets in English that include the given keywords" +
                " on the basis of filter.level");
        LOG.info("------------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreaming')" +
                "@source(type='twitter' , consumer.key='" + streamingConsumerKey + "'," +
                "consumer.secret='" + streamingConsumerSecret + "'," +
                "access.token ='" + streamingAccessToken + "'," +
                "access.token.secret='" + streamingAccessTokenSecret + "', " +
                "mode= 'streaming', track = 'Amazon', language = 'en', filter.level = 'none' ," +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags', keywords = 'track.words')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string, keywords string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(event);
                    eventArrived.set(true);

                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTwitterStreaming1")
    public void testTwitterStreaming2() throws InterruptedException {
        LOG.info("----------------------------------------------------------------------------------");
        LOG.info("TwitterSourceStreaming TestCase 2 - Filtering random sample of all public statuses.");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingSample')" +
                "@source(type='twitter' , consumer.key='" + streamingConsumerKey + "'," +
                "consumer.secret='" + streamingConsumerSecret + "'," +
                "access.token ='" + streamingAccessToken + "'," +
                "access.token.secret='" + streamingAccessTokenSecret + "', " +
                "mode= 'streaming' , @map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId'," +
                " text= 'text',hashtags = 'hashtags', keywords = 'track.words')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string, keywords string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTwitterStreaming2")
    public void testTwitterStreaming3() throws InterruptedException {
        LOG.info("---------------------------------------------------------------------------------------------");
        LOG.info("TwitterSourceStreaming TestCase 3 - Filtering tweets that include the given keywords" +
                " from a specific location");
        LOG.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingByLocation')" +
                "@source(type='twitter' , consumer.key='" + streamingConsumerKey + "'," +
                "consumer.secret='" + streamingConsumerSecret + "'," +
                "access.token ='" + streamingAccessToken + "'," +
                "access.token.secret='" + streamingAccessTokenSecret + "', " +
                "mode= 'streaming', track = 'google,Amazon', location = '-122.75,36.8,-121.75,37.8,-74,40,-73,41'," +
                " @map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(event);
                    eventArrived.set(true);

                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTwitterStreaming3")
    public void testTwitterStreaming4() throws InterruptedException {
        LOG.info("-------------------------------------------------------------------------------");
        LOG.info("TwitterSourceStreaming TestCase 4 - Filtering tweets that tweeted by a specific followers");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingByLocation')" +
                "@source(type='twitter' , consumer.key='" + streamingConsumerKey + "'," +
                "consumer.secret='" + streamingConsumerSecret + "'," +
                "access.token ='" + streamingAccessToken + "'," +
                "access.token.secret='" + streamingAccessTokenSecret + "', " +
                "mode= 'streaming', follow ='11348282,20536157,15670515,1719374,58561993,18139619'," +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        if (!eventArrived.get()) {
            LOG.info("No tweets tweeted by the given followers within the waitTime");
        }
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTwitterStreaming4")
    public void testTwitterStreaming5() throws InterruptedException {
        LOG.info("----------------------------------------------------------------------------------");
        LOG.info("TwitterStreaming TestCase 5 - Test for pause and resume method.");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingSample')" +
                "@source(type='twitter' , consumer.key='" + streamingConsumerKey + "'," +
                "consumer.secret='" + streamingConsumerSecret + "'," +
                "access.token ='" + streamingAccessToken + "'," +
                "access.token.secret='" + streamingAccessTokenSecret + "', " +
                "mode= 'streaming', @map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId'," +
                " text= 'text',hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        Collection<List<Source>> sources = siddhiAppRuntime.getSources();

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(eventCount + " . " + event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, 10000);
        Assert.assertTrue(eventArrived.get());
        sources.forEach(e -> e.forEach(Source::pause));

        LOG.info("Siddhi App paused.................................");
        eventArrived.set(false);
        eventCount.set(0);

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertFalse(eventArrived.get());
        sources.forEach(e -> e.forEach(Source::resume));
        LOG.info("Siddhi App Resumed...............................");
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class, dependsOnMethods = "testTwitterStreaming5")
    public void testTwitterStreaming6() {
        LOG.info("------------------------------------------------------------------------------------------------");
        LOG.info("TwitterStreaming TestCase 6 - Test for to check whether the parameters are valid for streaming" +
                " mode.");
        LOG.info("------------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingSample')" +
                "@source(type='twitter' , consumer.key='" + streamingConsumerKey + "'," +
                "consumer.secret='" + streamingConsumerSecret + "'," +
                "access.token ='" + streamingAccessToken + "'," +
                "access.token.secret='" + streamingAccessTokenSecret + "', " +
                "mode= 'streaming',track = 'google,amazon,apple' , until = '2018-04-8', " +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(eventCount + " . " + event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class, dependsOnMethods = "testTwitterStreaming6")
    public void testTwitterStreaming7() {
        LOG.info("----------------------------------------------------------------------------------");
        LOG.info("TwitterStreaming TestCase 7 - Test for filter.level Parameter validation");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingSample')" +
                "@source(type='twitter' , consumer.key='" + streamingConsumerKey + "'," +
                "consumer.secret='" + streamingConsumerSecret + "'," +
                "access.token ='" + streamingAccessToken + "'," +
                "access.token.secret='" + streamingAccessTokenSecret + "', " +
                "mode= 'streaming',track = 'google,amazon,apple', filter.level = 'high' ," +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(eventCount + " . " + event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppValidationException.class, dependsOnMethods = "testTwitterStreaming7")
    public void testTwitterStreaming8() {
        LOG.info("----------------------------------------------------------------------------------");
        LOG.info("TwitterStreaming TestCase 8 - Test for Location Parameter validation");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingSample')" +
                "@source(type='twitter' , consumer.key='" + streamingConsumerKey + "'," +
                "consumer.secret='" + streamingConsumerSecret + "'," +
                "access.token ='" + streamingAccessToken + "'," +
                "access.token.secret='" + streamingAccessTokenSecret + "', " +
                "mode= 'streaming',track = 'google,amazon,apple', location = '-122.75,36.8,-121.75'," +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(eventCount + " . " + event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class, dependsOnMethods = "testTwitterStreaming8")
    public void testTwitterStreaming9() {
        LOG.info("----------------------------------------------------------------------------------");
        LOG.info("TwitterStreaming TestCase 9 - Test for Follow Parameter validation");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingSample')" +
                "@source(type='twitter' , consumer.key='" + streamingConsumerKey + "'," +
                "consumer.secret='" + streamingConsumerSecret + "'," +
                "access.token ='" + streamingAccessToken + "'," +
                "access.token.secret='" + streamingAccessTokenSecret + "', " +
                "mode= 'streaming',track = 'google,amazon,apple', follow = '1ab5670515' ," +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(eventCount + " . " + event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class, dependsOnMethods = "testTwitterStreaming9")
    public void testForMandatoryParam() {
        LOG.info("----------------------------------------------------------------------------------");
        LOG.info("Twitter TestCase - Test for to check whether mandatory parameters are given or not.");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingSample')" +
                "@source(type='twitter' , consumer.key='" + consumerKey + "'," +
                "consumer.secret='" + consumerSecret + "'," +
                "access.token ='" + accessToken + "'," +
                "access.token.secret='" + accessTokenSecret + "', " +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(eventCount + " . " + event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class, dependsOnMethods = "testForMandatoryParam")
    public void testForModeParam() {
        LOG.info("----------------------------------------------------------------------------------");
        LOG.info("Twitter TestCase - Test for mode parameter validation");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingSample')" +
                "@source(type='twitter' , consumer.key='" + consumerKey + "'," +
                "consumer.secret='" + consumerSecret + "'," +
                "access.token ='" + accessToken + "'," +
                "access.token.secret='" + accessTokenSecret + "', " +
                "mode = 'Poll', " +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(eventCount + " . " + event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }


    @Test(dependsOnMethods = "testForModeParam")
    public void testTwitterPolling1() throws InterruptedException {
        LOG.info("---------------------------------------------------------------");
        LOG.info("TwitterSourcePolling TestCase 1 - Retrieving tweets in English containing " +
                "the extract phrase 'happy hour'.");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterPolling')" +
                "@source(type='twitter' , consumer.key='" + consumerKey + "'," +
                "consumer.secret='" + consumerSecret + "'," +
                "access.token ='" + accessToken + "'," +
                "access.token.secret='" + accessTokenSecret + "', " +
                "mode= 'polling', query = 'happy hour', " +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(eventCount + " . " + event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        if (!eventArrived.get()) {
            LOG.info("No tweets matched with the given query");
        }
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTwitterPolling1")
    public void testTwitterPolling2() throws InterruptedException {
        LOG.info("---------------------------------------------------------------");
        LOG.info("TwitterSourcePolling TestCase 2 - Retrieving tweets containing #Amazon from a " +
                "specific geocode");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterPolling')" +
                "@source(type='twitter' , consumer.key='" + consumerKey + "'," +
                "consumer.secret='" + consumerSecret + "'," +
                "access.token ='" + accessToken + "'," +
                "access.token.secret='" + accessTokenSecret + "', " +
                "mode= 'polling', query = '#Amazon', geocode = '43.913723261972855,-72.54272478125,150km'," +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        if (!eventArrived.get()) {
            LOG.info("No tweets matched with the given query");
        }
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTwitterPolling2")
    public void testTwitterPolling3() throws InterruptedException {
        LOG.info("---------------------------------------------------------------");
        LOG.info("TwitterSourcePolling TestCase 3 - Retrieving popular tweets, mentioning NASA");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterPolling')" +
                "@source(type='twitter' , consumer.key='" + consumerKey + "'," +
                "consumer.secret='" + consumerSecret + "'," +
                "access.token ='" + accessToken + "'," +
                "access.token.secret='" + accessTokenSecret + "', " +
                "mode= 'polling', query = '@NASA' ,result.type = 'popular' ,@map(type='keyvalue', " +
                "@attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text',hashtags = 'hashtags', " +
                "query = 'polling.query')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string, query string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTwitterPolling3")
    public void testTwitterPolling4() throws InterruptedException {
        LOG.info("----------------------------------------------------------------------------------");
        LOG.info("TwitterPolling TestCase 4 - Test for current state and restore method.");
        LOG.info("----------------------------------------------------------------------------------");
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String inStreamDefinition = "" +
                "@app:name('TwitterPolling')" +
                "@source(type='twitter' , consumer.key='" + consumerKey + "'," +
                "consumer.secret='" + consumerSecret + "'," +
                "access.token ='" + accessToken + "'," +
                "access.token.secret='" + accessTokenSecret + "', " +
                "mode= 'polling', query = '@NASA', @map(type='keyvalue', @attributes(createdAt = 'createdAt'," +
                " id = 'tweetId', text= 'text',hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(eventCount + " . " + event);
                    eventArrived.set(true);
                }
            }
        };
        siddhiAppRuntime.addCallback("query1", queryCallback);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived.get());

        //persisting
        siddhiAppRuntime.persist();

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        //restarting siddhi app
        siddhiAppRuntime.shutdown();

        eventArrived.set(false);
        eventCount.set(0);

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        siddhiAppRuntime.start();

        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        //shutdown siddhi app
        siddhiAppRuntime.shutdown();

        Assert.assertTrue(eventArrived.get());
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class, dependsOnMethods = "testTwitterPolling4")
    public void testTwitterPolling5() {
        LOG.info("---------------------------------------------------------------");
        LOG.info("TwitterSourcePolling TestCase 5 - Test for to check whether the given parameters are valid for" +
                " polling mode");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterPolling')" +
                "@source(type='twitter' , consumer.key='" + consumerKey + "'," +
                "consumer.secret='" + consumerSecret + "'," +
                "access.token ='" + accessToken + "'," +
                "access.token.secret='" + accessTokenSecret + "', " +
                "mode= 'polling', geocode = '43.913723261972855,-72.54272478125,150', follow ='15670515'," +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class, dependsOnMethods = "testTwitterPolling5")
    public void testTwitterPolling6() {
        LOG.info("---------------------------------------------------------------");
        LOG.info("TwitterSourcePolling TestCase 6 - Test for Query for polling mode without query parameter");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterPolling')" +
                "@source(type='twitter' , consumer.key='" + consumerKey + "'," +
                "consumer.secret='" + consumerSecret + "'," +
                "access.token ='" + accessToken + "'," +
                "access.token.secret='" + accessTokenSecret + "', " +
                "mode= 'polling', geocode = '43.913723261972855,-72.54272478125,150'," +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class, dependsOnMethods = "testTwitterPolling6")
    public void testTwitterPolling7() {
        LOG.info("---------------------------------------------------------------");
        LOG.info("TwitterSourcePolling TestCase 7 - Test for the geocode parameter validation.");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterPolling')" +
                "@source(type='twitter' , consumer.key='" + consumerKey + "'," +
                "consumer.secret='" + consumerSecret + "'," +
                "access.token ='" + accessToken + "'," +
                "access.token.secret='" + accessTokenSecret + "', " +
                "mode= 'polling', query = '#Amazon', geocode = '43.913723261972855,-72.54272478125,150'," +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = "testTwitterPolling7")
    public void testTwitterPolling8() {
        LOG.info("---------------------------------------------------------------");
        LOG.info("TwitterSourcePolling TestCase 8 - Test for the result.type parameter validation.");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterPolling')" +
                "@source(type='twitter' , consumer.key='" + consumerKey + "'," +
                "consumer.secret='" + consumerSecret + "'," +
                "access.token ='" + accessToken + "'," +
                "access.token.secret='" + accessTokenSecret + "', " +
                "mode= 'polling', query = '#Amazon', result.type = 'populaar'," +
                "@map(type='keyvalue', @attributes(createdAt = 'createdAt', id = 'tweetId', text= 'text'," +
                " hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(event);
                    eventArrived.set(true);
                }
            }
        });
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTwitterPolling8")
    public void testTwitterPolling9() throws InterruptedException {
        LOG.info("----------------------------------------------------------------------------------");
        LOG.info("TwitterPolling TestCase 9 - Test for pause and resume method.");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingSample')" +
                "@source(type='twitter' , consumer.key='" + consumerKey + "'," +
                "consumer.secret='" + consumerSecret + "'," +
                "access.token ='" + accessToken + "'," +
                "access.token.secret='" + accessTokenSecret + "', " +
                "mode= 'polling', query = '@NASA', @map(type='keyvalue', @attributes(createdAt = 'createdAt', " +
                "id = 'tweetId', text= 'text',hashtags = 'hashtags')))" +
                "define stream inputStream(createdAt String, id long, text String, hashtags string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        Collection<List<Source>> sources = siddhiAppRuntime.getSources();

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    LOG.info(eventCount + " . " + event);
                    eventArrived.set(true);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, 10000);
        Assert.assertTrue(eventArrived.get());
        sources.forEach(e -> e.forEach(Source::pause));

        LOG.info("Siddhi App paused.................................");
        eventArrived.set(false);
        eventCount.set(0);

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertFalse(eventArrived.get());
        sources.forEach(e -> e.forEach(Source::resume));
        LOG.info("Siddhi App Resumed...............................");
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }
}

