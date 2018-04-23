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

import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.Query;

/**
 * It creates the query for polling or streaming mode.
 */
public class QueryBuilder {
    private static String keywords;
    private static String queryParam;
    private QueryBuilder() {
    }

    public static Query createQuery (String q, int count, String language, long sinceId, long maxId, String until,
                              String since, String resultType, String geoCode, double latitude, double longitude,
                              double radius, String unitName) {
        Query query = new Query(q);
        queryParam = q;
        if (count > 0) {
            query.count(count);
        }
        if (!language.trim().isEmpty()) {
            query.lang(language);
        }
        if (!resultType.trim().isEmpty()) {
            query.resultType(Query.ResultType.valueOf(resultType));
        }
        if (!geoCode.trim().isEmpty()) {
            query.geoCode(new GeoLocation(latitude, longitude), radius, unitName);
        }
        if (!until.trim().isEmpty()) {
            query.until(until);
        }
        if (!since.trim().isEmpty()) {
            query.since(since);
        }
        query.sinceId(sinceId);
        query.maxId(maxId);

        return query;
    }

    public static FilterQuery createFilterQuery (String languageParam, String trackParam, long[] follow,
                                          String filterLevel, double[][] locations) {
        FilterQuery filterQuery = new FilterQuery();
        keywords = trackParam;
        if (!trackParam.trim().isEmpty()) {
            filterQuery.track(trackParam.split(TwitterConstants.DELIMITER));
        }

        if (!languageParam.trim().isEmpty()) {
            filterQuery.language(languageParam.split(TwitterConstants.DELIMITER));
        }

        if (follow != null) {
            filterQuery.follow(follow);
        }

        if (!filterLevel.trim().isEmpty()) {
            filterQuery.filterLevel(filterLevel);
        }

        if (locations != null) {
            filterQuery.locations(locations);
        }
        return filterQuery;
    }

    public static String getTrackParam() {
        return keywords;
    }

    public static String getQueryParam() {
        return queryParam;
    }
}
