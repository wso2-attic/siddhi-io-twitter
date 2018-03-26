package org.wso2.extension.siddhi.io.twitter.util;

import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.Query;

/**
 * It creates the query for polling or streaming mode.
 */

public class QueryBuilder {
    private QueryBuilder() {
    }

    public static Query createQuery (String q, int count, String language, long sinceId, long maxId, String until,
                              String since, String resultType, String geoCode, double latitude, double longitude,
                              double radius, String unitName) {
        Query query;
        query = new Query(q);
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
        FilterQuery filterQuery;
        String[] tracks;
        String[] filterLang;
        filterQuery = new FilterQuery();
        if (!trackParam.trim().isEmpty()) {
            tracks = Util.extract(trackParam);
            filterQuery.track(tracks);
        }

        if (!languageParam.trim().isEmpty()) {
            filterLang = Util.extract(languageParam);
            filterQuery.language(filterLang);
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

}
