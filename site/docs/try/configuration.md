# Configuration

Nessie is configurable via setting available properties as listed in the [application.properties](https://github.com/projectnessie/nessie/blob/main/servers/quarkus-server/src/main/resources/application.properties) file. 
These configuration settings are able to be set when starting up the docker image by 
adding them to the Docker invocation prefixed with `-D`.  For example, if you want to 
set Nessie to use the INMEMORY version store running on port 8080, you would run the 
following:

```bash
docker run -p 8080:8080 projectnessie/nessie \
  -Dnessie.version.store.type=INMEMORY \
  -Dquarkus.http.port=8080
```

## Core Nessie Configuration Settings

```properties
# which type of version store to use: JGIT, INMEMORY, DYNAMO. JGIT is for local testing, DYNAMO preferred for production
nessie.version.store.type=DYNAMO

# path if using JGIT
nessie.version.store.jgit.directory=/tmp/jgit

## Dynamo version store specific configuration
# should Nessie create its own dynamo tables
nessie.version.store.dynamo.initialize=false

## Dynamo Configuration
quarkus.dynamodb.aws.region=us-west-2
quarkus.dynamodb.aws.credentials.type=DEFAULT
# quarkus.dynamodb.endpoint-override=http://localhost:8000
```

## General Server Settings

```properties
# Quarkus settings
## Visit here for all configs: https://quarkus.io/guides/all-config
## some parameters are only configured at build time. These have been marked as such https://quarkus.io/guides/config#overriding-properties-at-runtime
quarkus.log.level=INFO

## Quarkus http related settings
quarkus.http.port=19120
quarkus.http.test-port=19121
quarkus.http.access-log.enabled=true
# fixed at buildtime
quarkus.resteasy.path=/api/v1
quarkus.resteasy.gzip.enabled=true

## Quarkus auth settings
#quarkus.oidc.credentials.secret=
#quarkus.oidc.client-id=
#quarkus.oidc.auth-server-url=
# fixed at buildtime
quarkus.http.auth.basic=false
quarkus.oidc.enabled=false


## Quarkus swagger settings
# fixed at buildtime
quarkus.swagger-ui.always-include=false
quarkus.swagger-ui.enable=false

## Quarkus monitoring and tracing settings
## jaeger specific settings
quarkus.jaeger.service-name=nessie
quarkus.jaeger.sampler-type=ratelimiting
quarkus.jaeger.sampler-param=1
#quarkus.jaeger.endpoint=http://localhost:14268/api/traces
# fixed at buildtime
quarkus.jaeger.metrics.enabled=true


## sentry specific settings
quarkus.log.sentry.level=ERROR
quarkus.log.sentry.in-app-packages=org.projectnessie
quarkus.log.sentry=false
#quarkus.log.sentry.dsn=https://<fillin>.ingest.sentry.io/<fillin>
```

!!! info
    A complete set of configuration options for Quarkus can be found on [quarkus.io](https://quarkus.io/guides/all-config)

### Metrics
Metrics are published using prometheus and can be collected via standard methods. See:
[Prometheus](https://prometheus.io).


### Swagger
The Swagger UI allows for testing the REST API and reading the API docs. It is available 
via [localhost:19120/swagger-ui](http://localhost:19120/swagger-ui)
