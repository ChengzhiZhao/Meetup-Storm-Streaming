# Longpull_Storm
This the Hackthon for Storm Part to consume data and push to ElasticSearch and Redis

#### 1. Install ElasticSearch and Kibana

#### 2. Open terminal and type "elasticsearch"
```bash
#To check ES is running
curl 'http://localhost:9200/?pretty'
```

#### 3. Start Kibana
```bash
kibana
```

#### 4. Install and start the Redis Server
```bash
redis-server
```

#### 5. Enable webdis
```bash
./webdis &
```

#### 6. Make sure you have the mapping for Elastic Search, it can be found at "es_mappings.json"

#### 7. Run LocalTopologyRunner