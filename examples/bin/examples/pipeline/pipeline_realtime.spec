[{
    "schema": {
        "dataSource": "pipelinestream",
        "aggregators": [
            {"type": "count", "name": "articles"},
            {"type": "doubleSum", "fieldName": "followers", "name": "total_followers"},
            {"type": "doubleSum", "fieldName": "following",  "name": "total_following" },
            {"type": "doubleSum", "fieldName": "status_updates",  "name": "total_status_updates" },

            {"type": "min", "fieldName": "followers", "name": "min_followers"},
            {"type": "max", "fieldName": "followers", "name": "max_followers"},

            {"type": "min", "fieldName": "following", "name": "min_following"},
            {"type": "max", "fieldName": "following", "name": "max_following"},

            {"type": "min", "fieldName": "status_updates", "name": "min_status_updates"},
            {"type": "max", "fieldName": "status_updates", "name": "max_status_updates"}
        ],
        "indexGranularity": "minute",
        "shardSpec": {"type": "none"}
    },

    "config": {
        "maxRowsInMemory": 50000,
        "intermediatePersistPeriod": "PT2m"
    },

    "firehose": {
        "type": "pipeline",
        "maxEventCount": 500000,
        "maxRunMinutes": 120,
        "webSocketUrl": "wss://pipeline.attensity.com/account/1/feed?api_key=1234567890ABCDEF1"
    },

    "plumber": {
        "type": "realtime",
        "windowPeriod": "PT3m",
        "segmentGranularity": "hour",
        "basePersistDirectory": "/tmp/example/twitter_realtime/basePersist"
    }
}]
