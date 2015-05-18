Kicking the tires on an attempt to do Logspout in Scala to see if the JVM Docker API client libraries are holding up.

Highly experimental.

```bash
$ mvn clean compile exec:java -Dexec.mainClass="com.sumologic.docker.App"
```