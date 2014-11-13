-record(vbucket_config, {
    num_vbuckets :: integer(),
    mask :: integer(),
    num_servers :: integer(),
    num_replicas :: integer(),
    user :: string(),
    password :: string(),
    servers :: [string()],
    vbuckets :: [string()],
    hash_algorithm = crc :: atom() 
}).
