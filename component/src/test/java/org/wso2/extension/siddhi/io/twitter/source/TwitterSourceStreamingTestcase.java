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
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class TwitterSourceStreamingTestcase {
    private static final Logger LOG = Logger.getLogger(TwitterSourceStreamingTestcase.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private volatile boolean eventArrived;
    private int waitTime = 50;
    private int timeout = 10000;

    @BeforeMethod
    public void init() {
        eventCount.set(0);
        eventArrived = false;
    }


    @Test
    public void testTwitterStreaming1() throws InterruptedException {
        LOG.info("------------------------------------------------------------------------------------------------");
        LOG.info("TwitterSourceStreaming TestCase 1 - Filtering tweets in English that include the given keywords");
        LOG.info("------------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreaming')" +
                "@source(type='twitter' , consumer.key='YPjsD5JYHYXJRsK4utYT1SN1b'," +
                "consumer.secret='fLn8uD6ECHE6ypXX70AgjuMRIzpRdcj6W6rS78cVVe1AF2GnnU'," +
                "access.token ='948469744398733312-uYqNO12cDxO27OIQeAlYxbL9e2kdjSp'," +
                "access.token.secret='t1DTGn2QAZG8SNgYwXur7ZojXh1TK10l6iVwrok68B7yW', " +
                "mode= 'streaming', track = 'Amazon,Google,Apple', language = 'en', " +
                "@map(type='json', fail.on.missing.attribute='false' ,@attributes(created_at = 'created_at'," +
                " id = 'id' ,id_str = 'id_str', text = 'text', coordinates='coordinates', user='user')))" +
                "define stream inputStream(created_at String, id long, id_str String, text String, " +
                "coordinates string, user string);";
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
                    eventArrived = true;

                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTwitterStreaming2() throws InterruptedException {
        LOG.info("----------------------------------------------------------------------------------");
        LOG.info("TwitterSourceStreaming TestCase 2 - Filtering random sample of all public statuses.");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingSample')" +
                "@source(type='twitter' , consumer.key='YPjsD5JYHYXJRsK4utYT1SN1b'," +
                "consumer.secret='fLn8uD6ECHE6ypXX70AgjuMRIzpRdcj6W6rS78cVVe1AF2GnnU'," +
                "access.token ='948469744398733312-uYqNO12cDxO27OIQeAlYxbL9e2kdjSp'," +
                "access.token.secret='t1DTGn2QAZG8SNgYwXur7ZojXh1TK10l6iVwrok68B7yW', " +
                "mode= 'streaming' ,@map(type='json', fail.on.missing.attribute='false' ," +
                "@attributes(created_at = 'created_at', id = 'id' ,id_str = 'id_str', text = 'text'," +
                " coordinates='coordinates', user='user')))" +
                "define stream inputStream(created_at String, id long, id_str String, text String, " +
                "coordinates string, user string);";
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
                    eventArrived = true;
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTwitterStreaming3() throws InterruptedException {
        LOG.info("---------------------------------------------------------------------------------------------");
        LOG.info("TwitterSourceStreaming TestCase 3 - Filtering tweets that include the given keywords" +
                " from a specific location");
        LOG.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingByLocation')" +
                "@source(type='twitter' , consumer.key='YPjsD5JYHYXJRsK4utYT1SN1b'," +
                "consumer.secret='fLn8uD6ECHE6ypXX70AgjuMRIzpRdcj6W6rS78cVVe1AF2GnnU'," +
                "access.token ='948469744398733312-uYqNO12cDxO27OIQeAlYxbL9e2kdjSp'," +
                "access.token.secret='t1DTGn2QAZG8SNgYwXur7ZojXh1TK10l6iVwrok68B7yW', " +
                "mode= 'streaming', track = 'Amazon,Google', location = '49.871159: -6.379880,55.811741:1.768960'," +
                " @map(type='json', fail.on.missing.attribute='false' ,@attributes(created_at = 'created_at'," +
                " id = 'id' ,id_str = 'id_str', text = 'text', coordinates='coordinates', user='user')))" +
                "define stream inputStream(created_at String, id long, id_str String, text String, " +
                "coordinates string, user string);";
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
                    eventArrived = true;

                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTwitterStreaming4() throws InterruptedException {
        LOG.info("-------------------------------------------------------------------------------");
        LOG.info("TwitterSourceStreaming TestCase 4 - Filtering tweets that tweeted by a specific followers");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingByLocation')" +
                "@source(type='twitter' , consumer.key='YPjsD5JYHYXJRsK4utYT1SN1b'," +
                "consumer.secret='fLn8uD6ECHE6ypXX70AgjuMRIzpRdcj6W6rS78cVVe1AF2GnnU'," +
                "access.token ='948469744398733312-uYqNO12cDxO27OIQeAlYxbL9e2kdjSp'," +
                "access.token.secret='t1DTGn2QAZG8SNgYwXur7ZojXh1TK10l6iVwrok68B7yW', " +
                "mode= 'streaming', follow ='11348282,20536157,15670515,17193794,58561993,18139619'," +
                " @map(type='json',fail.on.missing.attribute='false' ,@attributes(created_at = 'created_at'," +
                " id = 'id' ,id_str = 'id_str', text = 'text', coordinates='coordinates', user='user')))" +
                "define stream inputStream(created_at String, id long, id_str String, text String, " +
                "coordinates string, user string);";
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
                    eventArrived = true;

                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        if (!eventArrived) {
            LOG.info("No tweets tweeted by the given followers");
        }
        //Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTwitterStreaming5() throws InterruptedException {
        LOG.info("----------------------------------------------------------------------------------");
        LOG.info("TwitterStreaming TestCase 5 - Test for pause and resume method.");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterStreamingSample')" +
                "@source(type='twitter' , consumer.key='YPjsD5JYHYXJRsK4utYT1SN1b'," +
                "consumer.secret='fLn8uD6ECHE6ypXX70AgjuMRIzpRdcj6W6rS78cVVe1AF2GnnU'," +
                "access.token ='948469744398733312-uYqNO12cDxO27OIQeAlYxbL9e2kdjSp'," +
                "access.token.secret='t1DTGn2QAZG8SNgYwXur7ZojXh1TK10l6iVwrok68B7yW', " +
                "mode= 'streaming' ,@map(type='json', fail.on.missing.attribute='false' ," +
                "@attributes(created_at = 'created_at', id = 'id' ,id_str = 'id_str', text = 'text'," +
                " coordinates='coordinates', user='user')))" +
                "define stream inputStream(created_at String, id long, id_str String, text String, " +
                "coordinates string, user string);";
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
                    LOG.info(event);
                    eventArrived = true;
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        //LOG.info(eventCount);
        Assert.assertTrue(eventArrived);
        sources.forEach(e -> e.forEach(Source::pause));
        Thread.sleep(200);
        //LOG.info(eventCount + " , " + eventArrived);
        eventArrived = false;
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        //LOG.info(eventCount + " , " + eventArrived);
        Assert.assertFalse(eventArrived);
        sources.forEach(e -> e.forEach(Source::resume));
        Thread.sleep(500);
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        //LOG.info(eventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testTwitterPolling1() throws InterruptedException {
        LOG.info("---------------------------------------------------------------");
        LOG.info("TwitterSourcePolling TestCase 1 - Retrieving tweets in English containing " +
                "the extract phrase 'happy hour'.");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterPolling')" +
                "@source(type='twitter' , consumer.key='YPjsD5JYHYXJRsK4utYT1SN1b'," +
                "consumer.secret='fLn8uD6ECHE6ypXX70AgjuMRIzpRdcj6W6rS78cVVe1AF2GnnU'," +
                "access.token ='948469744398733312-uYqNO12cDxO27OIQeAlYxbL9e2kdjSp'," +
                "access.token.secret='t1DTGn2QAZG8SNgYwXur7ZojXh1TK10l6iVwrok68B7yW', " +
                "mode= 'polling', query = 'happy hour' , language = 'en', " +
                "geocode = '44.467186,-73.214804,2500km'" +
                " ,@map(type='json', fail.on.missing.attribute='false' ,@attributes(created_at = 'created_at'," +
                " id = 'id',id_str = 'id_str', text = 'text', coordinates='coordinates', user='user')))" +
                "define stream inputStream(created_at String, id long, id_str String, text String, " +
                "coordinates string, user string);";
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
                    eventArrived = true;
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        //Assert.assertTrue(eventArrived);
        if (!eventArrived) {
            LOG.info("No tweets matched with the given query");
        }
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTwitterPolling2() throws InterruptedException {
        LOG.info("---------------------------------------------------------------");
        LOG.info("TwitterSourcePolling TestCase 2 - Retrieving tweets containing #Amazon from a " +
                "specific geocode");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterPolling')" +
                "@source(type='twitter' , consumer.key='YPjsD5JYHYXJRsK4utYT1SN1b'," +
                "consumer.secret='fLn8uD6ECHE6ypXX70AgjuMRIzpRdcj6W6rS78cVVe1AF2GnnU'," +
                "access.token ='948469744398733312-uYqNO12cDxO27OIQeAlYxbL9e2kdjSp'," +
                "access.token.secret='t1DTGn2QAZG8SNgYwXur7ZojXh1TK10l6iVwrok68B7yW', " +
                "mode= 'polling', query = '#Amazon', result.type = 'popular', until = '2018-02-26'," +
                " @map(type='json', fail.on.missing.attribute='false' ,@attributes(created_at = 'created_at'," +
                " id = 'id' ,id_str = 'id_str', text = 'text', coordinates='coordinates', user='user')))" +
                "define stream inputStream(created_at String, id long, id_str String, text String, " +
                "coordinates string,user string);";
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
                    eventArrived = true;
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTwitterPolling3() throws InterruptedException {
        LOG.info("---------------------------------------------------------------");
        LOG.info("TwitterSourcePolling TestCase 3 - Retrieving popular tweets tweeted by NASA");
        LOG.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@app:name('TwitterPolling')" +
                "@source(type='twitter' , consumer.key='YPjsD5JYHYXJRsK4utYT1SN1b'," +
                "consumer.secret='fLn8uD6ECHE6ypXX70AgjuMRIzpRdcj6W6rS78cVVe1AF2GnnU'," +
                "access.token ='948469744398733312-uYqNO12cDxO27OIQeAlYxbL9e2kdjSp'," +
                "access.token.secret='t1DTGn2QAZG8SNgYwXur7ZojXh1TK10l6iVwrok68B7yW', " +
                "mode= 'polling', query = '@NASA' ,result.type = 'popular' ,@map(type='json'," +
                " fail.on.missing.attribute='false' ,@attributes(created_at = 'created_at', id = 'id' " +
                ",id_str = 'id_str', text = 'text', coordinates='coordinates', user='user')))" +
                "define stream inputStream(created_at String, id long, id_str String, text String, " +
                "coordinates string, user string);";
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
                    eventArrived = true;
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}

