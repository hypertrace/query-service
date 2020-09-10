# Query Service
The Query Service interfaces with Apache Pinot Data Store

## Description

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/hypertrace-query-arch.png) | 
|:--:| 
| *Hypertrace Query Architecture* |

- Query Service serves time series data for attributes/metrics from spans and events. The query interface this exposes is more like a table where you can select any columns, aggregations on them with filters. It's easy to do slicing and dicing of data (only from one table since no JOINs are supported) with this interface.
- Currently Pinot is the only DB layer for this service and queries are directly translated to PQL. However, in future we could add support for more data stores like Presto, Druid etc here.
- This layer doesn't understand the entities and their relationships because the query interface is generic table like interface.

## Building locally
The Query service uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build Query Service, run:

```
./gradlew dockerBuildImages
```
## Testing

### Running unit tests
Run `./gradlew test` to execute unit tests. 


### Testing image

You can test the image you built after modification by running docker-compose or helm setup. 

#### docker-compose

`Note:` 
- For docker-compose setup we use [hypertrace-service](https://github.com/hypertrace/hypertrace-service) which contains `query-service`. So to test changes with docker-compose you can checkout branch with your changes for `query-service` in [hypertrace-service](https://github.com/hypertrace/hypertrace-service) repo and build a new image to use with docker-compose setup. [This issue will be gone ones we switch to using macro-repo]

After that just Change the tag for `hypertrace-service` from `:main` to `:test` in [docker-compose file](https://github.com/hypertrace/hypertrace/blob/main/docker/docker-compose.yml) like this.

```yaml
  hypertrace-service:
    image: hypertrace/hypertrace-service:test
    container_name: hypertrace-service
    ...
```

and then run `docker-compose up` to test the setup.

## Docker Image Source:
- [DockerHub > Query service](https://hub.docker.com/r/hypertrace/Query-service)
